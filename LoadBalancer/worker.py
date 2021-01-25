from multiprocessing import Event
from LoadBalancer.job import Job
import json
import zmq
import time
import uuid
import random

# Worker parameters
WORKER_HOST = "127.0.0.1"
WORKER_PORT = 5755


class Worker(object):
    """
    Send a message when connected of the form {'worker_id': 'xxx', 'message': 'connect'}.
    Accept work in the form of the job object class {'number1': xxx, 'number2': xxx}, square the number1,
    sum the number2 and send it back to the controller in the form
    {'worker_id': 'xxx', 'message': 'job_done', 'job': job_payload}.
    Where job_payload has the information about the result.


    Parameters
    ----------
        event : Event
            Event object of multiprocessing used for terminating the processes.
        host: str
            Worker host connection
        port: int
            Worker port connection

    """

    def __init__(
        self, event: Event = Event(), host: str = WORKER_HOST, port: int = WORKER_PORT
    ):
        self.stop_event = event
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.host = host
        self.port = port
        self.socket_id = uuid.uuid4().hex[:4]
        self.socket.setsockopt_string(zmq.IDENTITY, self.socket_id)
        self.socket.connect("tcp://%s:%d" % (self.host, self.port))

        print("Worker %s connected" % self.socket_id)

        self._run()

    def _do_work(self, work):
        result = work["number1"] ** 2 + work["number2"]
        time.sleep(random.randint(1, 10))
        return result

    def _disconnect(self):
        """Send the Controller a disconnect message close connections."""
        self.socket.send_json({"worker_id": self.socket_id,"message": "disconnect"})
        self.socket.close()
        self.context.term()
        exit()

    def _run(self):
        """
        Run the worker until the event is terminated.
        """
        try:
            # Send a connect message
            self.socket.send_json({"worker_id": self.socket_id, "message": "connect"})
            while not self.stop_event.is_set():
                result = self.socket.recv_multipart()
                job = json.loads(result[0].decode("utf-8"))
                value = self._do_work(job)
                self.socket.send_json(
                    {
                        "worker_id": self.socket_id,
                        "message": "job_done",
                        "job": Job.get_result(job, value)
                    }
                )
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
        finally:
            self._disconnect()


if __name__ == "__main__":
    Worker()
