from multiprocessing import Event
import zmq
import time
import uuid
import random


class Worker(object):
    """Accept work in the form of {'number': xxx}, square the number and
    send it back to the controller in the form
    {'result': xxx, 'worker_id': yyy}. Our "work" in this case is just
    squaring the contents of 'number'.
    """

    def __init__(self, stop_event):
        self.stop_event = stop_event
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        # We don't need to store the id anymore, the socket will handle it
        # all for us.
        self.socket.setsockopt_string(zmq.IDENTITY, uuid.uuid4().hex[:4])
        # self.socket.identity = bytes(uuid.uuid4().hex[:4], encoding="latin-1")
        self.socket.connect('tcp://127.0.0.1:5755')

        self._run()

    def _do_work(self, work):
        result = work['number'] ** 2
        time.sleep(random.randint(1, 10))
        return result

    def _disconnect(self):
        """Send the Controller a disconnect message and end the run loop.
        """
        # self.stop_event.set()
        self.socket.send_json({'message': 'disconnect'})
        exit()

    def _run(self):
        """
        Run the worker forever. 
        We can use the multiprocessing events to better manage its termination events
        """
        try:
            # Send a connect message
            self.socket.send_json({'message': 'connect'})
            # print("connected")
            # Poll the socket for incoming messages. This will wait up to
            # 0.1 seconds before returning False. The other way to do this
            # is is to use zmq.NOBLOCK when reading from the socket,
            # catching zmq.AGAIN and sleeping for 0.1.
            while not self.stop_event.is_set():
                # if self.socket.poll(0):
                # print("waiting")
                # Note that we can still use send_json()/recv_json() here,
                # the DEALER socket ensures we don't have to deal with
                # client ids at all.
                result = self.socket.recv_multipart()
                # job_id, work = self.socket.recv_json()
                # job_id, work = result
                job_id, work = eval(result[0].decode("utf-8"))
                # print(type(result))
                # print(len(result))
                # print(result)
                print("Received %s with work %s" % (job_id, work))
                self.socket.send_json(
                    {'message': 'job_done',
                        'result': self._do_work(work),
                        'job_id': job_id})
        except Exception as e:
            print(e)
        finally:
            self._disconnect()


if __name__ == "__main__":
    event = Event()
    worker = Worker(event)
    # worker.run()
