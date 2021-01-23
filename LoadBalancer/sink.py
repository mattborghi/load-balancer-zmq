from multiprocessing import Event
import zmq

SINK_HOST = "127.0.0.1"
SINK_PORT = 5758


class Sink(object):
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
            self._disconnect()


if __name__ == "__main__":
    try:
        sink = Sink()
    except KeyboardInterrupt:
        # sink.stop_event.set()
        exit()
    finally:
        print("\nClosed Sink")