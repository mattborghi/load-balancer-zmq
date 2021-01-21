# Load Balancing General implementation

Implementation of a general load balancing pattern.

We will one Client generating tasks connected to a Broker Proxy which distributes the work across workers. After each worker finish their job they send the results to a Sink. This Sink returns the results back to the Client.

The Proxy must distribute the tasks evenly between workers and in case of worker failure its remaining tasks will be sent to other workers.

# Install

```sh
pipenv install git+https://github.com/mattborghi/load-balancer-zmq/#egg=LoadBalancer
```

# Local development instructions

Set working environment

```sh
pipenv install
pipenv shell
```

## A. Together

```sh
python main.py
```

## B. Separately

> **CAUTION**: First edit `LoadBalancer/controller.py` file

Run controller

```sh
python LoadBalancer/controller.py
```

Run this line for each worker you want to spawn in a separater terminal

```sh
python LoadBalancer/worker.py
```