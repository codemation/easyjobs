![](./images/logo.png)

A jobs framework for managing and  distributing  async / non-async tasks 

## Quick Start

    $ virtualenv -p python3.7 easy-job-env

    $ source easy-jobs-env/bin/activate

    (easy-rpc-env)$ pip install easyjobs
#
Documentation: [easyjobs documentation](https://codemation.github.io/easyjobs/)

#

## Usage - Jobs Manager
```python
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
```bash
$ uvicorn --host <host_address> --port <tcp_port> job_manager:server
```
## Basic Usage - Worker

```python

import os, time
import asyncio
from fastapi import FastAPI
from easyjobs.workers.worker import EasyJobsWorker

server = FastAPI()

@server.on_event('startup')
async def setup():
    worker = await EasyJobsWorker.create(
        server,
        server_secret='abcd1234',
        manager_host='0.0.0.0',
        manager_port=8220,
        manager_secret='abcd1234',
        jobs_queue='ETL',
    )

    every_minute = '* * * * *'
    default_args = {'args': ['http://stats']}

    async def get_data(url):
        return {'a': 1, 'b': 2, 'c': 3}
    async def load_db(data: dict):
        await db.tables['transformed'].insert(**data)

    @worker.task(run_after='transform', schedule=every_minute, default_args=default_args)
    async def extract(url: str):
        data = await get_data(url)
        return {'data': data}
    
    @worker.task(run_after='load')
    async def transform(data: dict):
        for k in data.copy():
            data[k] = int(data[k]) + 2
        return {'data': data}

    @worker.task(on_failure='failure_notify')
    async def load(data):
        await load_db(data)
        return f"data loaded"

    @worker.task()
    async def failure_notify(job_failed):
        await send_email('admin@company.io', job_failed)
        return job_failed

    os.environ['WORKER_TASK_DIR'] = '/mnt/subprocesses'

    @worker.task(subprocess=True)
    async def compute(data: dict):
        pass

```
Start Worker - With 5 Workers

```Bash
$ uvicorn --host <host_addr> --port <port> job_worker:server --workers=5
```
### Try it out"
    visit Job Manager uri: 
    http://0.0.0.0:8220/docs
<br>

![](./docs/images/easyjobs_openapi.png)