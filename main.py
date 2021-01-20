from multiprocessing import Process, Event
from LoadBalancer.client import Client
from LoadBalancer.worker import Worker
from LoadBalancer.controller import Controller
import time

# Set number of clients and workers
NBR_CLIENTS = 1
NBR_WORKERS = 5


def start(task, *args):
    """
    Generic function to load task on different Processes
    """
    process = Process(target=task, args=args)
    process.daemon = True
    process.start()


def start_stack(event):
    # Start controller
    start(Controller, event)
    # Start background tasks
    for i in range(NBR_CLIENTS):
        pass
        # start(client_task, event, i)
    for i in range(NBR_WORKERS):
        start(Worker, event)  # , i


if __name__ == "__main__":
    event = Event()
    start_stack(event)
    while not event.is_set():
        time.sleep(5)
        # Client(event)
