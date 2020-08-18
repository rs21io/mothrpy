# mothrpy

![GitHub](https://img.shields.io/github/license/rs21io/mothrpy)

## Installation
`pip install mothrpy`

If you need to create a listener you need to install the optional listener
dependencies

`pip install mothrpy[listener]`

## Usage

```python
from mothrpy import JobRequest

request = JobRequest(service='echo')
request.add_parameter(value='Hello Mothr!')
result = request.run_job()
print(result)
```
