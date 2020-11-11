# mothrpy

![GitHub](https://img.shields.io/github/license/rs21io/mothrpy)
![Actions](https://github.com/rs21io/mothrpy/workflows/tests/badge.svg)
[![codecov](https://codecov.io/gh/rs21io/mothrpy/branch/main/graph/badge.svg)](https://codecov.io/gh/rs21io/mothrpy)

## Installation
`pip install mothrpy`

## Usage

Basic example submitting a job request

```python
from mothrpy import JobRequest

request = JobRequest(service='echo')
request.add_parameter(value='Hello MOTHR!')
result = request.run_job()
print(result)
```

Submitting a job request using `MothrClient`. This allows you to reuse the
client connection when making multiple requests.

```python
from mothrpy import JobRequest, MothrClient

client = MothrClient()

# Send one request
request = JobRequest(client=client, service='echo')
request.add_parameter(value='Hello MOTHR!')
result = request.run_job()
print(result)

# Reuse the client in another request
request = JobRequest(client=client, service='echo')
request.add_parameter(value='Hello again MOTHR!')
result = request.run_job()
print(result)
```
