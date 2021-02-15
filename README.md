![](./images/logo.png)

A jobs framework for managing and  distributing  async / non-async tasks 

## Quick Start

    $ virtualenv -p python3.7 easy-job-env

    $ source easy-jobs-env/bin/activate

    (easy-rpc-env)$ pip install easyjobs

## Supported Brokers - Pull Jobs
- rabbitmq
- easyjobs
- TODO - Amazon SQS

## Supported Producers
- rabbitmq - Send jobs to rabbitmq first - consume later
- EasyJobs API - Create jobs directly via EasyJobsManager API

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

import asyncio, os
from easyjobs.manager import EasyJobsManager
from fastapi import FastAPI

server = FastAPI()

#Optional - Specify job-manager.db location
os.environ['DB_PATH'] = '/mnt/jobs_database/'

@server.on_event('startup')
async def startup():

    job_manager = await EasyJobsManager.create(
        server,
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
        server_secret='abcd1234',
        manager_host='192.168.1.18',
        manager_port=8220,
        manager_secret='abcd1234',
        jobs_queue='DEFAULT',
        max_tasks_per_worker=3
    )

    @worker.task()
    async def work_a(a, b, c):
        await asyncio.sleep(5)
        return {'result': [a, b, c]}
```
Start Worker - With 5 Workers

```Bash
$ uvicorn --host <host_addr> --port <port> job_worker:server --workers=5
```
To test it out - visit job manager uri: Example:
http://0.0.0.0:8220/docs
<br>

![](./images/easyjobs_openapi.png)

#
## Register Tasks
Tasks can be registered on a Manager or Worker by using referencing the <instance>.task decorator / function. 
<br>
### Task register arguments:

- <b>namespace</b> - Manager only, Defaults to 'DEFAULT' - Determines what queue task is registered within, methods can be registered within multiple namespaces. Workers inherit jobs_queue, from creation. 
- <b>on_failure</b>  - Default Unspecified - Will attempt to create with on_failure=<task_name> if task run resulted in a failure
- <b>retry_policy</b>  - Defaults retry_policy='retry_once',  with possible values [retry_always, never]
- run_after</b>  - Defaults Unspecified - Will create job with run_after=<task_name> using results of current task as argument for run_afer task.
- <b>subprocess</b>  - Defaults False - Defines whether a task should be created via a subprocess 
<br>

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
@manager.task(namespace="general", run_after='more_general_work')
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
#
## Note: Considerations with non-async Tasks
<em> 
Worker tasks which do not contain I/O bound tasks (Netowrk,  Web Requests / Database querries ) and run beyond 10 seconds, should be placed within a task subprocess definition. This is to allow the current worker thread continue servicing other concurrent tasks. 
</em>

#
Tasks created with subprocess=True, will create a new process (using an separate & uncontended python GIL), run until completed / failed, and then report the results back to EasyJobsManager. EasyJobsManager will provide the results to the worker, releasing a task reservation ( allowing more work to complete using results).  

#
## Subprocess usage with Blocking Code

```python
#worker

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
- Methods registered with 'subprocess=True' do not contain logic
- Arguments improve readability, but do not affect functionality (except in [template](https://github.com/codemation/easyjobs/blob/main/easyjobs/workers/task_subprocess.py).)

## Subprocess Template - Example
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
 
## Creating Jobs
Jobs can be created 1 of 2 ways:
#
### EasyJobsManager API
![](./images/api_0.png)

- Methods registered locally for connected workers will be visible from OpenAPI and can be invoked via REST Method. 
- Invocation of a job via the API will return a request_id indicating the request was added to the persistent queue. 
- A job will be created as soon as a worker is able to respond to the job creation request. 
- Created Jobs are then added to a secondary queue with the associated job parameters(run_after, retry_policy, etc..)
- Workers with free task reservations will start and run job ASAP.

The jobs visible in the OpenAPI are dynamic added, even after startup. Newly added Workers Namespaces or registered Functions will be visible by simply refreshing the /docs page.
<br><br>
![](./images/api_1.png)

An important feature of EasyJobs is signature cloning of registered functions locally & for remote workers. This allows for immediate argument verification before the request is queued. 


#
### See Also -  Adding Jobs to a Message Queue
See [Producers](https://github.com/codemation/easyjobs/tree/main/easyjobs/producers) - to review how to create jobs.

#
## Job Life Cycle
1. A Job is created if pulled from a Message Queue or added via the EasyJobsManager API. 
2. The Job is added to the jobs database and queued for worker consumtion. 
3. A Job is selected by a worker and invoked with the provided args / kwargs parameters( if any ).
4. Job Failures result in triggering a retry followed by any task on_failure tasks ( if any ), then reported as failed to EasyJobsManager's results database / queue.
5. Job Successes result in creating any task run_after tasks using the results of the last job, then reporting the results to EasyJobsManager's results database / queue.
6. Results are stored within the EasyJobsManager Database 

## Consuming Results
When a job request is created via the API, a request_id is returned right-away.

![](./images/api_2.png)

A result can be consumed / querried via the Pull Job Result API. <b>Pull implies Removal - Only with this in mind - otherwise use <u>View Job Result</u></b>

![](./images/api_3.png)

Pull Job Result will wait up to 5 seconds for a job to complete, returning the consumed result.

#
## Creating Jobs via Message Queues

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

#
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