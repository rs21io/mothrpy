# mothrpy

[![pipeline status](http://gitlab.rs21.io/mothr/client-libraries/mothrpy/badges/master/pipeline.svg)](http://gitlab.rs21.io/mothr/client-libraries/mothrpy/commits/master)
[![coverage report](http://gitlab.rs21.io/mothr/client-libraries/mothrpy/badges/master/coverage.svg)](http://gitlab.rs21.io/mothr/client-libraries/mothrpy/commits/master)

## Installation
`pip install mothrpy`

If you need to create a listener you need to install the optional listener
dependencies

`pip install mothrpy[listener]`

## Usage

```python
from mothrpy import JobRequest

request = JobRequest(service='echo', url='https://mothr.rs21.io/api')
request.add_parameter(value='Hello Mothr!')
result = request.run_job()
print(result)
```
