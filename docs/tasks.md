#
### Tasks

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


### Task Flow
![](./images/task-flow.png)

### Registering Tasks
Tasks can be registered on a Manager or Worker by using referencing the <instance>.task decorator / function. 
```python
# task arguments
def task(
    namespace: str = 'DEFAULT',
    on_failure: Optional[str] = None, # on failure job
    retry_policy: Optional[str] =  'retry_once', # retry_once, retry_always, never
    run_before: Optional[list] = None,
    run_after: Optional[list] = None,
    subprocess: Optional[bool] = False,
    schedule: Optional[str] = None,
    default_args: Optional[dict] = None,
) -> Callable:
```
<br>

### Task register arguments:

- <b>namespace</b> - Manager only, Defaults to 'DEFAULT' - Determines what queue task is registered within, methods can be registered within multiple namespaces. Workers inherit jobs_queue, from creation. 
- <b>on_failure</b>  - Default Unspecified - Will attempt to create with on_failure=<task_name> if task run resulted in a failure
- <b>retry_policy</b>  - Defaults retry_policy='retry_once',  with possible values [retry_always, never]
- <b>run_before</b> - List - Runs listed jobs in parelell before starting task.
- <b>run_after</b>  - Defaults Unspecified - Will create job with run_after=<task_name> using results of current task as argument for run_afer task.
- <b>subprocess</b>  - Defaults False - Defines whether a task should be created via a subprocess 
- <b>schedule</b> - Default Unspecified - Define a cron schedule which the Job Manager will invoke the Task automatically
<br>
- <b>default_args</b> - Default Unspecified - Required if task takes arguments, and used when a task is invoked via schedule.

```python
@worker.task(on_failure='send_failure_email')
async def finance_work(employee_id: str, employee_data: dict):
    """
    do finance work
    """
    return finance_results

@worker.task(retry_policy='always')
async def send_failure_email(reason):
    # send email with reason
    return f"email sent for {reason}"
```

```python
@manager.task(namespace="general", run_after=['more_general_work'])
async def general_work(general_data: dict):
    """
    do general work
    """
    return general_results

@manager.task(namespace="general")
async def more_general_work(general_results):
    # extra work on general_results
    return f"more_general_results"
```
!!! TIP
    Worker tasks which do not contain I/O bound tasks (Network,  Web Requests / Database querries ) and run beyond 10 seconds, should be placed within a task subprocess definition. This is to allow the current worker thread continue servicing other concurrent tasks

!!! NOTE
    Tasks created with <b>subprocess=True</b>, will create a new process (using an separate & uncontended python GIL), run until completed / failed, and then report the results back to EasyJobsManager. EasyJobsManager will provide the results to the worker, releasing a task reservation ( allowing more work to complete using results).  

### Subprocess Usage

#### Example - Worker

```python

#required env vars
os.environ['WORKER_TASK_DIR'] = '/home/codemation/blocking_funcs/'

@worker.task(subprocess=True)
async def basic_blocking(a, b, c):
    pass   
```
```bash
$ ls /home/codemation/blocking_funcs
advanced_blocking.py basic_blocking.py 
```
#### Example - Manager
```python
# manager

#required env vars
os.environ['MANAGER_HOST'] = '0.0.0.0'
os.environ['MANAGER_PORT'] = '8220'
os.environ['WORKER_TASK_DIR'] = /home/codemation/blocking_funcs/

@manager.task(subprocess=True)
async def advanced_blocking(a, b, c):
    pass   
```
!!! TIP
    - Methods registered with 'subprocess=True' do not contain logic
    - Arguments improve readability, but do not affect functionality (except in [template](https://github.com/codemation/easyjobs/blob/main/easyjobs/workers/task_subprocess.py))
### Subprocess Template
```
# /home/codemation/blocking_funcs/basic_blocking.py
import time
from easyjobs.workers.task import subprocess

@subprocess
def work(a, b, c):
    """
    insert blocking / non-blocking work here
    """
    time.sleep(5) # Blocking
    return {'result': 'I slept for 5 seconds - blocking with {a} {b} {c}'}

if __name__ == '__main__':
    work()
```

!!! NOTE 
    The name method work() is not ultimately significant, but shold match what appears within the (if __name__ == '__main__': ) block of code: