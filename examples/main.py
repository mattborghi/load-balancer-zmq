import logging
import sys
import os

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
PACKAGE_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT))
logging.info("Adding %s to the sys path in order to load the package.\n\n" % PACKAGE_DIR)
sys.path.append(PACKAGE_DIR)


from multiprocessing import Process, Event
from LoadBalancer.client import Client, Workload
from LoadBalancer.worker import Worker
from LoadBalancer.controller import Controller
from LoadBalancer.sink import Sink

import time

# Set number of clients and workers
# NBR_CLIENTS = 1
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
    # Start Sink
    start(Sink, event)
    # Start Client workload
    start(Workload, event)
    # Start workers
    for _ in range(NBR_WORKERS):
        start(Worker, event)


if __name__ == "__main__":
    event = Event()
    start_stack(event)
    while not event.is_set():
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            pass