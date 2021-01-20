from multiprocessing import Event
import zmq


class Client(object):
    def __init__(self, event):
        self.stop_event = event
        self._run()

    def _run(self):
        self.stop_event.set()


if __name__ == "__main__":
    event = Event()
    Client(event)
