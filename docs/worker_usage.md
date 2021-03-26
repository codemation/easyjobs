## Worker

### Usage

```python
import asyncio, os
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
        max_tasks_per_worker=5
    )

    every_minute = '* * * * *'
    default_args = {'kwargs': {'url': ['http://stats']}}

    async def get_data(url):
        print(f"get_data: {url}")
        return {'a': 1, 'b': 2, 'c': 3}
    async def load_db(data: dict):
        #await db.tables['transformed'].insert(**data)
        return f"data {data} loaded to db"
    async def send_email(address: str, message: str):
        return f"email sent to {address}"

    @worker.task(run_after=['transform'], schedule=every_minute, default_args=default_args)
    async def extract(url: str):
        print(f"extract started")
        data = await get_data(url)
        print(f"extract finished")
        return {'data': data}
    
    @worker.task(run_after=['load'])
    async def transform(data: dict):
        print(f"transform started")
        for k in data.copy():
            data[k] = int(data[k]) + 2
        print(f"transform finished")
        return {'data': data}

    @worker.task(on_failure='failure_notify', run_after=['compute'])
    async def load(data):
        print(f"load started")
        await load_db(data)
        print(f"load finished")
        return {'data': data}

    @worker.task()
    async def failure_notify(job_failed):
        await send_email('admin@company.io', job_failed)
        return job_failed

    @worker.task()
    async def deploy_environment():
        print(f"deploy_environment - started")
        await asyncio.sleep(5)
        print(f"deploy_environment - completed")
        return f"deploy_environment - completed"

    @worker.task()
    async def prepare_db():
        print(f"prepare_db - started")
        await asyncio.sleep(5)
        print(f"prepare_db - completed")
        return f"prepare_db - completed"

    @worker.task(run_before=['deploy_environment', 'prepare_db'])
    async def configure_environment():
        print(f"configure_environment - starting")
        await asyncio.sleep(5)
        print(f"configure_environment - finished")
        return f"configure_environment - finished"

    os.environ['WORKER_TASK_DIR'] = '/home/josh/Documents/python/easyjobs'

    @worker.task(subprocess=True, run_before=['configure_environment'])
    async def compute(data: dict):
        pass

    @worker.task()
    async def pipeline():
        print(f"pipline started")
        result = await compute(data={'test': 'data'})
        print(f"pipline - result is {result} - finished")
        return result

```
### Starting a Worker
Start Worker - With 5 workers

```Bash
$ uvicorn --host <host_addr> --port <port> job_worker:server --workers=5
```
!!! Success "Try it out - Visit the Running Job Manager URL"
    http://0.0.0.0:8220/docs
<br>