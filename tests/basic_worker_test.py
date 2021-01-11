import os, time
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
        #debug=True
    )

    @worker.task(run_after='worker_b')
    async def worker_a(a, b, c):
        await asyncio.sleep(5)
        return {'a': a, 'b': b, 'c': c}
    
    @worker.task(run_after='worker_c')
    async def worker_b(a, b, c):
        await asyncio.sleep(5)
        return {'a': a + 2, 'b': b + 2, 'c': c +2}

    @worker.task(on_failure='failure_notify')
    async def worker_c(a, b, c):
        await asyncio.sleep(5)
        result = {'a': a + 2, 'b': b + 2, 'c': c +2}
        print('worker_c ', result)
        raise Exception(f"fake error: {result}")

    @worker.task()
    async def failure_notify(job_failed):
        worker.log.error(job_failed)
        return job_failed

    # 'WORKER_PORT', 'WORKER_PATH', 'WORKER_TASK_DIR'
    os.environ['WORKER_PORT'] = '8221'
    os.environ['WORKER_PATH'] = '/ws/jobs'
    os.environ['WORKER_TASK_DIR'] = '/home/tso/Documents/python/easyjobs/easyjobs/'

    @worker.task(subprocess=True)
    async def basic_blocking(a, b, c):
        pass