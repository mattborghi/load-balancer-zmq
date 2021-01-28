# Load Balancing General implementation

Implementation of a general load balancing pattern.

This pattern has one Client generating tasks connected to a Broker Proxy which distributes the work across workers. After each worker finish their job they send the results to a Sink. This Sink returns these results back to the Client.

The Proxy must distribute the tasks evenly between workers and in case of worker failure their remaining tasks will be sent to other workers.

There is also a [Binary Star](https://zguide.zeromq.org/docs/chapter4/#High-Availability-Pair-Binary-Star-Pattern) pattern that allows us to always have an available Proxy.

# Install

```properties
pipenv install git+https://github.com/mattborghi/load-balancer-zmq/#egg=LoadBalancer
```

or 

```properties
pipenv update
```

if the package is already present on the `Pipenv` file.

# Local development instructions

Set working environment

```properties
pipenv install
pipenv shell
```

## A. Together

```properties
python main.py
```

## B. Separately

Run controller

```properties
python LoadBalancer/controller.py
```

Run this line for each worker you want to spawn in a separater terminal

```properties
python LoadBalancer/worker.py
```

Run Sink

```properties
python LoadBalancer/sink.py
```

# Generate release files with name based on setup.py

```properties
python setup.py sdist bdist_wheel
```
