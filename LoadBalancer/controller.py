from multiprocessing import Event
from copy import copy
import zmq
import json
import time
import uuid


# MAX_WORK_PER_SESSION = 50
MAX_JOBS_PER_WORKER = 2


class Controller(object):
    def __init__(self, event):
        self.stop_event = event
        self.context = zmq.Context()
        self.backend = self.context.socket(zmq.ROUTER)
        self.backend.bind("tcp://127.0.0.1:5755")
        # print("connected to %s" % self.backend)

        # Connect to Client
        self.frontend = self.context.socket(zmq.ROUTER)
        self.frontend.connect("tcp://127.0.0.1:5754")

        # We'll keep our workers here, this will be keyed on the worker id,
        # and the value will be a dict of Job instances keyed on job id.
        self.workers = {}
        # TODO: Include self.workers info in an Logger object
        self.statistics = Logger()

        # We won't assign more than 50 jobs to a worker at a time; this ensures
        # reasonable memory usage, and less shuffling when a worker dies.
        self.max_jobs_per_worker = MAX_JOBS_PER_WORKER
        self._work_to_requeue = []
        # Use the same socket to receive the results as we are using a ROUTER socket
        self.backend_result = self.backend

        self.poller = zmq.Poller()

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
        worker_id, work = sorted(self.workers.items(), key=lambda x: len(x[1]))[0]
        # print("Assigned %d jobs to %s" % (len(work), worker_id))
        if len(work) < self.max_jobs_per_worker:
            return worker_id
        # No worker is available. Our caller will have to handle this.
        return None

    def _process_results(self, worker_id, job_id, result):
        print(
            "Worker ID %s finished job %s with result %s" % (worker_id, job_id, result)
        )
        self.statistics.add(worker_id)

    def _before_finishing(self):
        # print("Remaining work %s" % self.workers)
        remaining_jobs = self.workers.values()
        if not any(remaining_jobs):
            print("There are pending jobs")
        self.statistics.show_results()
        # This should finish all the workers/client/controller
        self.stop_event.set()

    def _handle_worker_message(self, worker_id, message):
        """Handle a message from the worker identified by worker_id.

        {'message': 'connect'}
        {'message': 'disconnect'}
        {'message': 'job_done', 'job_id': 'xxx', 'result': 'yyy'}
        """
        if message["message"] == "connect":
            assert worker_id not in self.workers
            # print("Connected new worker: %s" % worker_id)
            self.workers[worker_id] = {}
        elif message["message"] == "disconnect":
            remaining_work = self.workers.pop(worker_id)
            # print("Remaining work %s" % remaining_work)
            for k, v in remaining_work.items():
                v["id"] = k
                self._work_to_requeue.append(v)
        elif message["message"] == "job_done":
            # print("Work done")
            result = message["result"]
            job_id = message["job_id"]
            del self.workers[worker_id][job_id]
            self._process_results(worker_id, job_id, result)

    def _close_connections(self):
        self.frontend.close()
        self.backend.close()
        self.context.term()

    def _run(self):
        # Only poll for requests from backend until workers are available
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.frontend, zmq.POLLIN)
        try:
            while not self.stop_event.is_set():
                sockets = dict(self.poller.poll())

                if self.backend in sockets:
                    # print("BACKEND")
                    worker_id, message = self.backend.recv_multipart()
                    # print(
                    #     "Received message %s from worker id %s"
                    #     % (message.decode("utf-8"), worker_id.decode("utf-8"))
                    # )
                    worker_id = worker_id.decode("utf-8")
                    message = json.loads(message.decode("utf-8"))
                    self._handle_worker_message(worker_id, message)

                if self.frontend in sockets:
                    # print("FRONTEND")
                    _, payload = self.frontend.recv_multipart()
                    request = json.loads(payload.decode("utf-8"))
                    if request["message"] == "connect":
                        # print("Received from client id %s" % request["client_id"])
                        # print("Job: %s" % request["job"])
                        # append job to task list
                        self._work_to_requeue.append(request["job"])
                    elif request["message"] == "disconnect":
                        pass
                        # print("Client id %s disconnected" % request["client_id"])
                    else:
                        Exception("Unhandled client message")

                # Run tasks
                next_worker_id = self._get_next_worker_id()
                # if a worker is available and also a task is waiting to be processed
                if next_worker_id and self._work_to_requeue:
                    # print("worker available! %s" % next_worker_id)
                    job = self._work_to_requeue.pop(0)
                    # print("processing job: %s" % job)
                    # self.backend.send_multipart([next_worker_id, job])
                    self.backend.send_string(next_worker_id, flags=zmq.SNDMORE)
                    self.backend.send_json(job)
                    payload = copy(job)
                    job_id = payload.pop("id")
                    self.workers[next_worker_id][job_id] = payload
        except KeyboardInterrupt:
            pass
            # print("Crtl+c IN CONTROLLER")
        except Exception as e:
            print(e)
        finally:
            # if self.stop_event.is_set():
            self._close_connections()
            self._before_finishing()


if __name__ == "__main__":
    from log_info import Logger

    event = Event()
    controller = Controller(event)
    # controller.run()
else:
    from LoadBalancer.log_info import Logger
