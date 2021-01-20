from multiprocessing import Process, Event
from src.client import Client
from src.worker import Worker
from src.controller import Controller
import time

# Set number of clients and workers
NBR_CLIENTS = 1
NBR_WORKERS = 3


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
    while True:
        time.sleep(10)
        Client(event)
