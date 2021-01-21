from multiprocessing import Event
import zmq
import json
import time
import uuid


MAX_WORK_PER_SESSION = 50
MAX_JOBS_PER_WORKER = 2

class Controller(object):
    def __init__(self, event):
        self.stop_event = event
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind("tcp://127.0.0.1:5755")
        # print("connected to %s" % self.socket)
        # We'll keep our workers here, this will be keyed on the worker id,
        # and the value will be a dict of Job instances keyed on job id.
        self.workers = {}
        # TODO: Include self.workers info in an Logger object
        self.statistics = Logger()

        # We won't assign more than 50 jobs to a worker at a time; this ensures
        # reasonable memory usage, and less shuffling when a worker dies.
        self.max_jobs_per_worker = MAX_JOBS_PER_WORKER
        self._work_to_requeue = {}
        # Use the same socket to receive the results as we are using a ROUTER socket
        self.socket_result = self.socket

        self._run()

    def _get_next_worker_id(self):
        """Return the id of the next worker available to process work. Note
        that this will return None if no clients are available.
        """
        # It isn't strictly necessary since we're limiting the amount of work
        # we assign, but just to demonstrate that we could have any
        # algorithm here that we wanted we'll find the worker with the least
        # work and try that.
        # print("Workers available: %s" % self.workers)
        # print(self.workers.items())
        if not self.workers.items():
            # print("no items")
            return None
        worker_id, work = sorted(self.workers.items(),
                                 key=lambda x: len(x[1]))[0]
        # print("Assigned %d jobs to %s" % (len(work), worker_id))
        if len(work) < self.max_jobs_per_worker:
            return worker_id
        # No worker is available. Our caller will have to handle this.
        return None

    def work_iterator(self):
        # iter() makes our xrange object into an iterator so we can use
        # next() on it.
        iterator = iter(range(0, MAX_WORK_PER_SESSION))
        while True:
            # Return requeued work first. We could simplify this method by
            # returning all new work then all requeued work, but this way we
            # know _work_to_requeue won't grow too large in the case of
            # many disconnects.
            if self._work_to_requeue:
                # Get an elemnent from the stack
                name = list(self._work_to_requeue.keys())[0]
                # print("Names")
                # print(self._work_to_requeue.keys())
                elem = self._work_to_requeue.pop(name)
                yield Job(elem, name)
            else:
                try:
                    yield Job({"number": next(iterator)})
                except StopIteration:
                    # print("Stopped Iteration")
                    yield None

    def _process_results(self, worker_id, job_id, result):
        print("Worker ID %s finished job %s with result %s" %
              (worker_id, job_id, result))
        self.statistics.add(worker_id)

    def _before_finishing(self):
        # Wait for the remaing jobs to finish
        # print("Remaining jobs to finish")
        # print(self.workers.values())
        # print("Remaining work: %d" % len(self.workers.values()))
        # print(self.workers)
        if not any(self.workers.values()):
            self.statistics.show_results()
            # This should finish all the workers/client/controller
            self.stop_event.set()
            # break

    def _handle_worker_message(self, worker_id, message):
        """Handle a message from the worker identified by worker_id.

        {'message': 'connect'}
        {'message': 'disconnect'}
        {'message': 'job_done', 'job_id': 'xxx', 'result': 'yyy'}
        """
        if message["message"] == "connect":
            assert worker_id not in self.workers
            self.workers[worker_id] = {}
        elif message["message"] == "disconnect":
            remaining_work = self.workers.pop(worker_id)
            # print("Remaining work")
            # print(remaining_work)
            # Remove the worker so no more work gets added, and put any
            # remaining work into _work_to_requeue
            # self._work_to_requeue.extend(remaining_work.values())
            self._work_to_requeue = self._work_to_requeue | remaining_work  # NOTE: 3.9+ ONLY
            # print("Work to reque")
            # print(self._work_to_requeue)
        elif message["message"] == "job_done":
            # print("Work done")
            result = message["result"]
            job_id = message["job_id"]
            # print("Workers for id: %s" % worker_id)
            # print(self.workers[worker_id])
            # print("Deleting job %s" % message["job_id"])
            # job = self.workers[worker_id].pop([message["job_id"]])
            del self.workers[worker_id][job_id]
            # _process_results() is just a trivial logging function so I've
            # omitted it from here, but you can find it in the final source
            # code.
            self._process_results(worker_id, job_id, result)
    
    def _close_connections(self):
        self.socket.close()
        self.context.term()

    def _run(self):
        for job in self.work_iterator():
            next_worker_id = None
            while next_worker_id is None:
                # First check if there are any worker messages to process. We
                # do this while checking for the next available worker so that
                # if it takes a while to find one we're still processing
                # incoming messages.
                while self.socket_result.poll(0):
                    # Note that we're using recv_multipart() here, this is a
                    # special method on the ROUTER socket that includes the
                    # id of the sender. It doesn't handle the json decoding
                    # automatically though so we have to do that ourselves.
                    worker_id, message = self.socket_result.recv_multipart()
                    message = json.loads(message)
                    self._handle_worker_message(worker_id, message)
                # If there are no available workers (they all have 50 or
                # more jobs already) sleep for half a second.
                next_worker_id = self._get_next_worker_id()
                if next_worker_id is None:
                    time.sleep(0.5)
            
            if self.stop_event.is_set():
                # print("Stopping event")
                self._close_connections()
                break
            if not job:
                self._before_finishing()
                continue
            # We've got a Job and an available worker_id, all we need to do
            # is send it. Note that we're now using send_multipart(), the
            # counterpart to recv_multipart(), to tell the ROUTER where our
            # message goes.
            # print("Sending to %s message %s" % (next_worker_id, job))
            data = json.dumps((job.id, job.work))
            # print(data)
            self.socket.send_multipart([next_worker_id, data.encode("utf-8")])
            # self.socket.send(next_worker_id, flags = zmq.SNDMORE)
            # self.socket.send_json(data)
            self.workers[next_worker_id][job.id] = job.work
            # print("Changed worker job")
            # print(self.workers[next_worker_id])


class Job(object):
    def __init__(self, work, id=None):
        self.id = id if id else uuid.uuid4().hex
        self.work = work


if __name__ == "__main__":
    from log_info import Logger
    event = Event()
    controller = Controller(event)
    # controller.run()
else:
    from LoadBalancer.log_info import Logger
