## Manager

A EasyJobsManager can be setup to pull work from / add to an EasyJobs message queue or a ampq supported message queue( RabbitMq)

### EasyJobs - Broker

```python
# Manager - Jobs Runner
# job_manager.py

import asyncio, os
from easyjobs.manager import EasyJobsManager
from fastapi import FastAPI

server = FastAPI()

os.environ['DB_PATH'] = '/mnt/jobs_database/'

@server.on_event('startup')
async def startup():

    job_manager = await EasyJobsManager.create(
        server,
        server_secret='abcd1234'
    )

```
### RabbitMq - Broker
Requires the most amount of work to get started with, if in doubt use the EasyJobs queue. 

### Pre-Requisites
- Running rabbitmq message queue

### Usage

```python
# Manager - Jobs Runner
# job_manager.py

import asyncio
from easyjobs.manager import EasyJobsManager
from fastapi import FastAPI

server = FastAPI()

@server.on_event('startup')
async def startup():

    job_manager = await EasyJobsManager.create(
        server,
        server_secret='abcd1234',
        broker_type='rabbitmq',
        broker_path='amqp://guest:guest@127.0.0.1/'
    )

    @job_manager.task()
    async def basic_job(arg1, arg2, arg3, *args):
        print(f"basic_job: {arg1} {arg2} {arg3} - args {args}")
        await asyncio.sleep(2)
        return arg1, arg2, arg3
```

### Start EasyJobsManager

```bash
$ uvicorn --host <host_address> --port <tcp_port> job_manager:server
```

### Easyjobs Manager - API
![](./images/ETL_API.png)