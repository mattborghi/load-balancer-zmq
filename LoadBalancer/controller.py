from multiprocessing import Event
from copy import copy
import zmq
import json

# Controller parameters
MAX_JOBS_PER_WORKER = 2
FRONTEND_HOST = "127.0.0.1"
FRONTEND_PORT = 5754
BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 5755


class Controller(object):
    """
    Proxy class for the Load Balancer pattern.

    Parameters
    ----------
        event : Event
            Event object of multiprocessing used for terminating the processes.
        backend_host: str
            Controller's backend host connection
        backend_port: int
            Controller's backend port connection
        frontend_host: str
            Controller's frontend host connection
        frontend_port: int
            Controller's frontend port connection

    """

    def __init__(
        self,
        event: Event = Event(),
        backend_host: str = BACKEND_HOST,
        backend_port: int = BACKEND_PORT,
        frontend_host: str = FRONTEND_HOST,
        frontend_port: int = FRONTEND_PORT,
    ):
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.frontend_host = frontend_host
        self.frontend_port = frontend_port
        self.stop_event = event
        self.context = zmq.Context()
        self.backend = self.context.socket(zmq.ROUTER)
        self.backend.bind("tcp://%s:%d" % (self.backend_host, self.backend_port))

        # Connect to Client
        self.frontend = self.context.socket(zmq.ROUTER)
        self.frontend.connect("tcp://%s:%d" % (self.frontend_host, self.frontend_port))

        # We'll keep our workers here, this will be keyed on the worker id,
        # and the value will be a dict of Job instances keyed on job id.
        self.workers = {}

        self.statistics = Logger()

        # We won't assign more than MAX_JOBS_PER_WORKER jobs to a worker at a time;
        # this ensures reasonable memory usage, and less shuffling when a worker dies.
        self.max_jobs_per_worker = MAX_JOBS_PER_WORKER
        self._work_to_requeue = []
        # Use the same socket to receive the results as we are using a ROUTER socket
        self.backend_result = self.backend

        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.frontend, zmq.POLLIN)
        self._run()

    def _get_next_worker_id(self):
        """Return the id of the next worker available to process work. Note
        that this will return None if no clients are available.

        The next available worker is selected as the worker with less job assigned
        with the caveat that they have assined less than their respective max_jobs_per_worker.
        """
        # It isn't strictly necessary since we're limiting the amount of work
        # we assign, but just to demonstrate that we could have any
        # algorithm here that we wanted we'll find the worker with the least
        # work and try that.
        if not self.workers.items():
            return None
        worker_id, work = sorted(self.workers.items(), key=lambda x: len(x[1]))[0]
        if len(work) < self.max_jobs_per_worker:
            return worker_id
        # No worker is available. Our caller will have to handle this.
        return None

    def _process_results(self, worker_id: str, job_id: str, result) -> None:
        """
        - Simple logger that prints the output to the screen when a new result is obtained.
        - Also adds to Logger object information about finish tasks.

        Args:
            worker_id (str): Worker Id
            job_id (str): Job Id
            result (number): Result of job
        """
        print(
            "Worker ID %s finished job %s with result %s" % (worker_id, job_id, result)
        )
        self.statistics.add(worker_id)

    def _before_finishing(self):
        """Tasks done before finishing"""
        remaining_jobs = self.workers.values()
        if not any(remaining_jobs):
            print("There are pending jobs")
        self.statistics.show_results()
        # This should finish all the workers/client/controller
        self.stop_event.set()

    def _handle_worker_message(self, worker_id: str, message: dict) -> None:
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
            for k, v in remaining_work.items():
                v["id"] = k
                self._work_to_requeue.append(v)
        elif message["message"] == "job_done":
            result = message["result"]
            job_id = message["job_id"]
            del self.workers[worker_id][job_id]
            self._process_results(worker_id, job_id, result)

    def _handle_client_message(self, request: dict) -> None:
        """
        Handle a message from the client.
        We don't need to identify them to a client_id as we run with only 1 client.

        {'message': 'connect', 'job': job_payload}
        {'message': 'disconnect'}

        job_payload is another dict which is an instance of the Job class.

        """

        if request["message"] == "connect":
            # append job to task list
            self._work_to_requeue.append(request["job"])
        elif request["message"] == "disconnect":
            pass
        else:
            Exception("Unhandled client message")

    def _close_connections(self):
        """Close connections"""
        self.frontend.close()
        self.backend.close()
        self.context.term()

    def _run(self):
        """
        Main function. Run the Proxy until the event stop is fired.
        Receive message fron the frontend or backend
        """
        try:
            while not self.stop_event.is_set():
                sockets = dict(self.poller.poll())

                if self.backend in sockets:
                    worker_id, message = self.backend.recv_multipart()
                    worker_id = worker_id.decode("utf-8")
                    message = json.loads(message.decode("utf-8"))
                    self._handle_worker_message(worker_id, message)

                if self.frontend in sockets:
                    # TODO: Get the client_id and send it to _handle_client_message
                    # if we want to handle several clients
                    _, payload = self.frontend.recv_multipart()
                    request = json.loads(payload.decode("utf-8"))
                    self._handle_client_message(request)

                # Run tasks
                next_worker_id = self._get_next_worker_id()
                # if a worker is available and also a task is waiting to be processed
                if next_worker_id and self._work_to_requeue:
                    job = self._work_to_requeue.pop(0)
                    self.backend.send_string(next_worker_id, flags=zmq.SNDMORE)
                    self.backend.send_json(job)
                    payload = copy(job)
                    job_id = payload.pop("id")
                    self.workers[next_worker_id][job_id] = payload
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
        finally:
            self._close_connections()
            self._before_finishing()


if __name__ == "__main__":
    from log_info import Logger

    Controller()
else:
    from LoadBalancer.log_info import Logger
