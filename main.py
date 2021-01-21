from multiprocessing import Process, Event
from LoadBalancer.client import Client, Workload
from LoadBalancer.worker import Worker
from LoadBalancer.controller import Controller
import time

# Set number of clients and workers
NBR_CLIENTS = 1
NBR_WORKERS = 2


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
    # Start Client workload
    start(Workload, event)
    # Start workers
    for i in range(NBR_WORKERS):
        start(Worker, event)  # , i


if __name__ == "__main__":
    event = Event()
    start_stack(event)
    while not event.is_set():
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            event.set()
    print("Wait 5 seconds before quitting main script")
    time.sleep(5)