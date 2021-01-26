from multiprocessing import Event
from time import sleep
import zmq

SINK_HOST = "127.0.0.1"
SINK_PORT = 5758


class Sink(object):
    """
    Pull work from the proxy of the form of the job object class 
    {'id': xxx, 'name': 'xxx', 'result': xxx, 'number1': xxx, 'number2': xxx}.


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
        self, event: Event = Event(), host: str = SINK_HOST, port: int = SINK_PORT
    ):
        self.stop_event = event
        self.context = zmq.Context()

        # Socket to receive messages on
        self.receiver = self.context.socket(zmq.PULL)
        self.host = host
        self.port = port
        self.receiver.bind("tcp://%s:%d" % (self.host, self.port))

        self.results = []

        self._run()
    
    def _disconnect(self):
        self.receiver.close()
        self.context.term()
        # is this really necessary?
        exit()

    def _before_finishing(self):
        sleep(1)
        print("\n\n")
        print("Results processed in Sink")
        print(self.results)

    def _run(self):
        try:
            while not self.stop_event.is_set():
                # Wait for start of batch
                s = self.receiver.recv_json()
                self.results.append(s)

        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
        finally:
            self._before_finishing()
            self._disconnect()


if __name__ == "__main__":
    try:
        Sink()
    except KeyboardInterrupt:
        # sink.stop_event.set()
        exit()
    finally:
        print("\nClosed Sink")