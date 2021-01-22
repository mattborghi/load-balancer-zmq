from multiprocessing import Event
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
    Send a messaage when connected of the form {'message': 'connect'}.
    Accept work in the form of {'number1': xxx, 'number2': xxx}, square the number1,
    sum the number2 and send it back to the controller in the form
    {'message': 'job_done','result': xxx, 'job_id': yyy}.


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
        self.socket.setsockopt_string(zmq.IDENTITY, uuid.uuid4().hex[:4])
        self.socket.connect("tcp://%s:%d" % (self.host, self.port))

        print("Worker %s connected" % self.socket.getsockopt_string(zmq.IDENTITY))

        self._run()

    def _do_work(self, work):
        result = work["number1"] ** 2 + work["number2"]
        time.sleep(random.randint(1, 10))
        return result

    def _disconnect(self):
        """Send the Controller a disconnect message close connections."""
        self.socket.send_json({"message": "disconnect"})
        self.socket.close()
        self.context.term()
        exit()

    def _run(self):
        """
        Run the worker until the event is terminated.
        """
        try:
            # Send a connect message
            self.socket.send_json({"message": "connect"})
            while not self.stop_event.is_set():
                result = self.socket.recv_multipart()
                job = json.loads(result[0].decode("utf-8"))
                job_id = job["id"]
                self.socket.send_json(
                    {
                        "message": "job_done",
                        "result": self._do_work(job),
                        "job_id": job_id,
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
