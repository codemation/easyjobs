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
        encryption_enabled=False,
        broker_type='rabbitmq',
        broker_path='amqp://guest:guest@127.0.0.1/',
        #debug=True
    )

    @job_manager.task()
    async def basic_job(arg1, arg2, arg3, *args):
        print(f"basic_job: {arg1} {arg2} {arg3} - args {args}")
        await asyncio.sleep(2)
        return arg1, arg2, arg3

    jobs = []
    for i in range(1, 5):
        jobs.append(
            await basic_job(i+1, i+2, i+3)
        )
    results = []
    for job in jobs:
        results.append(
            await job_manager.get_job_result(job)
        )
    print(f"results: {results}")