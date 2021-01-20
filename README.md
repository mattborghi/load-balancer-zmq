# Load Balancing General implementation

Implementation of a general load balancing pattern.

We will one Client generating tasks connected to a Broker Proxy which distributes the work across workers. After each worker finish their job they send the results to a Sink. This Sink returns the results back to the Client.

The Proxy must distribute the tasks evenly between workers and in case of worker failure its remaining tasks will be sent to other workers.

# Running instructions

Set working environment

```sh
pipenv install
pipenv shell
```

## Together

```sh
python main.py
```

## Separately

Run controller

```sh
python src/controller.py
```

Run worker

```sh
python src/worker.py
```