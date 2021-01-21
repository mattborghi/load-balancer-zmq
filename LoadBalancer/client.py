from multiprocessing import Event
import zmq
import uuid
import time

NUMBER_OF_MESSAGES_SENT = 4
# If we use 0 we should see the jobs well distributed as we are simulating
# tasks that consume practicaly zero time
WAIT_TIME = 0


class Workload(object):
    """
    This object simulates the connections of a client
    each time it has to send a task
    """

    def __init__(self, event, jobs: int = NUMBER_OF_MESSAGES_SENT, wait_time: int = WAIT_TIME):
        self.jobs = jobs
        # iter() makes our xrange object into an iterator so we can use
        # next() on it.
        self.stop_event = event
        self.iterator = iter(range(0, self.jobs))
        self.wait = wait_time
        self._run()

    def _work_iterator(self, job) -> dict:
        # Send NUMBER_OF_MESSAGES_SENT or None
        # try:
        return Job({"number": job})
        # except StopIteration:
        # print("Stopped Iteration")
        # yield None

    def _run(self):
        for job in range(self.jobs):
            # event = Event()
            # print("Sending message #%d" % job)
            # print("with messsage %s" % message)
            message = next(self.iterator)
            # print("encapsulated to %s" % self._work_iterator(message))
            Client(self.stop_event, self._work_iterator(message))
            time.sleep(self.wait)


class Job(object):
    def __init__(self, work, id=None):
        self.id = id if id else uuid.uuid4().hex
        self.work = work

    def result(self):
        return {"id": self.id, "number": self.work["number"]}


class Client(object):
    """
    Client instance of a load balancer pattern
    """

    def __init__(self, event, data):
        self.stop_event = event
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.bind("tcp://127.0.0.1:5754")
        self.socket.setsockopt_string(zmq.IDENTITY, uuid.uuid4().hex)
        # Object to send
        self.data = data
        self._run()

    def _disconnect(self):
        """ Disconnect the Client """
        # self.stop_event.set()
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
    Workload(event, jobs=10, wait_time=1)
