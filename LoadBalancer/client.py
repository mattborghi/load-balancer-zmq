from multiprocessing import Event
import zmq
import uuid
import time

# Workload parameters
NUMBER_OF_MESSAGES_SENT = 4
# If we use 0 we should see the jobs well distributed as we are simulating
# tasks that consume practicaly zero time
WAIT_TIME = 0
# CLient parameters
CLIENT_HOST = "127.0.0.1"
CLIENT_PORT = 5754


class Workload(object):
    """
    This object simulates the connections of a client
    each time it has to send a task. That's a simulated workload.

    Parameters
    ----------
    event :
        Event object of multiprocessing used for terminating the processes.
        ```python
        from multiprocessing import Event
        event = Event()
        ```
    jobs : int
        Number of messages sent in the workload.
    wait_time: int
        Waiting time between each task sent.

    """

    def __init__(
        self, event, jobs: int = NUMBER_OF_MESSAGES_SENT, wait_time: int = WAIT_TIME
    ):
        self.stop_event = event
        self.jobs = jobs
        # iter() makes our xrange object into an iterator so we can use next() on it.
        self.iterator = iter(range(0, self.jobs))
        self.wait = wait_time
        self._run()

    def _work_iterator(self, number1, number2) -> dict:
        return Job({"number1": number1, "number2": number2})

    def _run(self):
        for _ in range(self.jobs):
            message = next(self.iterator)
            Client(self.stop_event, self._work_iterator(message, message / 2))
            time.sleep(self.wait)


class Job(object):
    """
    Define an object to be sent to the workers to be processed.

    Job sent are in the form of:

    {
        "id": uuid4
        "number1": number
        "number2": number
    }

    """

    def __init__(self, payload: dict, id=None) -> dict:
        self.id = id if id else uuid.uuid4().hex[:4]
        self.payload = payload

    def result(self):
        return {
            "id": self.id,
            "number1": self.payload["number1"],
            "number2": self.payload["number2"],
        }


class Client(object):
    """
    Client instance of the load balancer pattern.

    Parameters
    ----------
        event :
            Event object of multiprocessing used for terminating the processes.
            ```python
            from multiprocessing import Event
            event = Event()
            ```
        data : dict
            A Job object
        host: str
            Client host connection
        port: int
            Client port connection
    """

    def __init__(
        self, event, data: dict, host: str = CLIENT_HOST, port: int = CLIENT_PORT
    ):
        self.stop_event = event
        self.context = zmq.Context()
        self.host = host
        self.port = port
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.bind("tcp://%s:%d" % (self.host, self.port))
        self.socket.setsockopt_string(zmq.IDENTITY, uuid.uuid4().hex)
        # Object to send over the sockets
        self.data = data
        self._run()

    def _disconnect(self):
        """ Disconnect the Client """
        self.socket.send_json(
            {
                "client_id": self.socket.getsockopt_string(zmq.IDENTITY),
                "message": "disconnect",
            }
        )
        self.socket.close()
        self.context.term()

    def _run(self):
        """ Send a message through a ZMQ ROUTER socket """
        self.socket.send_json(
            {
                "client_id": self.socket.getsockopt_string(zmq.IDENTITY),
                "message": "connect",
                "job": self.data.result(),
            }
        )
        self._disconnect()


if __name__ == "__main__":
    # Send 10 jobs waiting 1 second between each message
    event = Event()
    Workload(event, jobs=10, wait_time=3)
