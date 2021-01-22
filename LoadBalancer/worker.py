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
    """Accept work in the form of {'number': xxx}, square the number and
    send it back to the controller in the form
    {'result': xxx, 'worker_id': yyy}. Our "work" in this case is just
    squaring the contents of 'number'.
    """

    def __init__(self, stop_event, host=WORKER_HOST, port=WORKER_PORT):
        self.stop_event = stop_event
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.host = host
        self.port = port
        self.socket.setsockopt_string(zmq.IDENTITY, uuid.uuid4().hex[:4])
        self.socket.connect("tcp://%s:%d" % (self.host, self.port))

        print("Worker %s connected" % self.socket.getsockopt_string(zmq.IDENTITY))

        self._run()

    def _do_work(self, work):
        result = work["number"] ** 2
        time.sleep(random.randint(1, 10))
        return result

    def _disconnect(self):
        """Send the Controller a disconnect message and end the run loop."""
        self.socket.send_json({"message": "disconnect"})
        self.socket.close()
        self.context.term()
        exit()

    def _run(self):
        """
        Run the worker forever.
        We can use the multiprocessing events to better manage its termination events
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
    event = Event()
    worker = Worker(event)
