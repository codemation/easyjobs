# easyjobs
A jobs framework for managing and  distributing  async / non-async tasks 

## Quick Start

    $ virtualenv -p python3.7 easy-job-env

    $ source easy-jobs-env/bin/activate

    (easy-rpc-env)$ pip install easyjobs

## Supported Brokers - Pull Jobs
- rabbitmq
- TODO - Amazon SQS

## Supported Producers
- rabbitmq - Send jobs to rabbitmq first - consume later
- jobproxy - Send jobs directly to an EasyJobsManager

## Basic Usage - With Broker

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
        '/ws/jobs',
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

## Basic Usage - No Broker

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
        '/ws/jobs',
        server_secret='abcd1234'
    )

    @job_manager.task()
    async def basic_job(arg1, arg2, arg3, *args):
        print(f"basic_job: {arg1} {arg2} {arg3} - args {args}")
        await asyncio.sleep(2)
        return arg1, arg2, arg3
```

Start Job Manager

    $ uvicorn --host <host_address> --port <tcp_port> job_manager:server

## Connect Worker

```python
# job_worker.py

import asyncio
from fastapi import FastAPI
from easyjobs.workers.worker import EasyJobsWorker

server = FastAPI()

@server.on_event('startup')
async def setup():
    worker = await EasyJobsWorker.create(
        server,
        '/ws/jobs',
        server_secret='abcd1234',
        manager_host='192.168.1.18',
        manager_port=8220,
        manager_secret='abcd1234',
        manager_path='/ws/jobs',
        jobs_queue='DEFAULT',
        task_workers=3
    )

    @worker.task()
    async def work_a(a, b, c):
        await asyncio.sleep(5)
        return {'result': [a, b, c]}
```

Start Worker - With 5 Workers

    $ uvicorn --host <host_addr> --port <port> job_worker:server --workers=5


## Register Tasks
Tasks can be registered on a Manager or Worker by using referencing the <instance>.task decorator / function. 
<br><br>
### task register arguments:

- namespace - Defaults to 'DEFAULT' - Determines what queue task is registered within, methods can be registered within multiple namespaces. 
- on_failure - Default Unspecified - Will attempt to create with on_failure=<task_name> if task run resulted in a failure
- retry_policy - Defaults retry_policy='retry_once',  with possible values [retry_always, never]
- run_after - Defaults Unspecified - Will create job with run_after=<task_name> using results of current task as argument for run_afer task.
- subprocess - Defaults False - Defines whether a task should be created via a subprocess 
<br>

Examples

```python
@worker.task(namespace='finance')
async def finance_work(employee_id: str, employee_data: dict):
    """
    do finance work
    """
    return finance_results

@manager.task()
async def general_work(general_data: dict):
    """
    do general work
    """
    return general_results
```

## A Note on Blocking Tasks ( Work that cannot sleep, or CPU bound)
Worker tasks which do not contain I/O bound tasks ( Web Requests /  Database querries ) and run beyond 10 seconds, should be placed within a task subprocess definition. This is to allow the current worker thread continue servicing other concurrent tasks. 
<br><br>
tasks created with subprocess=True, will create a new process (using an separate & uncontended python GIL), run until completed / failed, and then report the results back to the current EasyJobsManager. The EasyJobsManager will provide the results to the worker, unlocking the coroutine ( if more work work to complete using results).  


## subprocess usage & blocking code 

subprocess=True definitions 
- @worker.task(subprocess=True) require a 'WORKER_TASK_DIR' environment variable and a matching func_name.py within the given directory path.
- @manager.task(subprocess=True) require 'MANAGER_HOST', 'MANAGER_PORT', 'WORKER_TASK_DIR' env variables are required
```python
# job_worker.py 

os.environ['WORKER_TASK_DIR'] = '/home/codemation/blocking_funcs/'

@worker.task(subprocess=True)
async def basic_blocking(a, b, c):
    pass   
```
Note: task(subprocess=True) methods do not contain logic. All defined actions should be placed in WORKER_TASK_DIR using this [template](https://github.com/codemation/easyjobs/blob/main/easyjobs/workers/task_subprocess.py). 

Arguments are not required in this subprocess definition, but should match the template work() args for readability. 

<br>

```python
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

## Jobs

Jobs should be created in the following format, using json serializable data. If you can run json.dumps(data) on the data, you can use it in a job.


    # Job Format 

    job = {
        'namespace': 'name' # also known as queue 
        'name': 'name',
        'args': [args],
        'kwargs': {'kwarg': 'val'}
    }

Tip: Think about how you would invoke he job if local, then create the syntax using a Producer. <br><br>

When a Job is added ( either pulled from a broker, or pushed via producer) the job is first added to a persistent database, then added to a gloabal queue to be run by workers monitoring the queue. 

## Producers
See [Producers](https://github.com/codemation/easyjobs/tree/main/easyjobs/producers) - to review how to create jobs.

## Job Life Cycle
1. A Job is created if pulled from a Message Queue or directly added via a job_proxy producer to a EasyJobsManager. 
2. The Job is added to a Jobs Database queued for worker consumtion. 
3. A Job is selected by a worker and invoked with the provided arg / kwargs parameters( if any ).
4. Job Failures result in triggering a retry followed by any task on_failure tasks ( if any ), then reported as failed to EasyJobsManager's results database / queue.
5. Job Successes result in creating any task run_after tasks using the results of the last job, then reporting the results to EasyJobsManager's results database / queue.
6. Results are stored within the EasyJobsManager Queue 

## Consuming Results
When a Job is created via a jobs_proxy producer, a job_id is returned immedietly. This job_id can be used to pull later or awaited right away( this does NOT make the job start sooner ).

```python    
# results_consumer.py
import asyncio
from easyrpc.proxy import EasyRpcProxy

async def get_results(job_id):
    results_proxy = await EasyRpcProxy.create(
        '0.0.0.0',  # EasyJobsManager Host / IP
        '8220',     # EasyJobsManager Port
        '/ws/manager',
        server_secret='abcd1234',
        namespace='job_results'
    )
    return await results_proxy['get_job_result'](job_id)
```


## Terminology

### EasyJobsManager
- responsible for pulling jobs from a broker 
- adds jobs to persistent database & global queue
- provides workers access to global queue for pulling jobs
- provides workers ability to store results to persistent database which can be pulled or pushed to a specificed message queue. 
- can act as a worker if task is defined locally within namespace
- Should NOT be forked
<br>
Note: Work performed on a Manager should be as non-blocking as possible, since the main thread cannot be forked, long running / blocking code on a Manager will have adverse affects. When in doubt, put it on a separate worker.

### EasyJobsWorker
- Connects to a running EasyJobsManager and pulls jobs to run within a specified queue
- Runs Jobs and pushes results back to EasyJobsManager
- Process can be forked
