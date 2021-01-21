from multiprocessing import Event
import zmq
import time

NUMBER_OF_MESSAGES_SENT = 5

class Client(object):
    def __init__(self, event):
        self.stop_event = event
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.bind("tcp://127.0.0.1:5754")

        # iter() makes our xrange object into an iterator so we can use
        # next() on it.
        self.iterator = iter(range(0, NUMBER_OF_MESSAGES_SENT))

        self._run()

    def _disconnect(self):
        self.stop_event.set()
        self.socket.close()
        self.context.term()

    def _work_iterator(self):
        # Send NUMBER_OF_MESSAGES_SENT or None
        # try:
        yield {"number": next(self.iterator)}
        # except StopIteration:
            # print("Stopped Iteration")
            # yield None

    def _run(self):
        # Send a message every 5 seconds
        i = 0
        while not self.stop_event.is_set():
            # print(i)
            if i == NUMBER_OF_MESSAGES_SENT:
                # print(i, NUMBER_OF_MESSAGES_SENT)
                self._disconnect()
            else:
                print("Sending message")
                # BEFORE SENDING MESSAGE CHECK RESPONSE FROM LOAD BALANCER
                self.socket.send_json(next(self._work_iterator()))
                # self.socket.send(b"I AM THE CLIENT")
                i += 1
                time.sleep(5)            
        # self.stop_event.set()


if __name__ == "__main__":
    try:
        event = Event()
        client = Client(event)
        # client.run()
    except KeyboardInterrupt:        
        print("\nExited Client")
        # client.stop_event.set()
        exit()
