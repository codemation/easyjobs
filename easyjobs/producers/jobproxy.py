import asyncio
from easyrpc.proxy import EasyRpcProxy

class Channel:
    def __init__(self, channel_generator):
        self.channel_generator = channel_generator
        self.started = False
    @classmethod
    async def create(
        cls,
        channel_generator
    ):
        new_channel = cls(channel_generator)
        await new_channel.channel_generator.asend(None)
        new_channel.started = True
        return new_channel

    async def send_job(self, job: dict):
        #if not 'job' in job:
        #    raise Exception(f"no job key provided in message {job}")
        await self.channel_generator.asend(job)
        return
    async def send_jobs(self, job_list: list):
        for job in job_list:
            await self.send_job(job)
        return
    def __del__(self):
        asyncio.create_task(self.channel_generator.asend('finished'))

async def producer(
    manager_host: str,
    manager_port: str,
    manager_path: str,
    manager_secret: str,
    queue: str
):
    print(f"creating manager_proxy with manager_secret: {manager_secret}")
    manager_proxy = await EasyRpcProxy.create(
        manager_host,
        manager_port,
        manager_path,
        server_secret=manager_secret,
        namespace=f"{queue}",
        debug=True
    )

    await asyncio.sleep(2)
    message = yield 'starting'

    if message == 'finished':
        yield 'finished'
            
    while True:

        print(f"producer waiting for messages")
        message = yield
        if message == 'finished':
            break
        print(f"producer received message: {message}")
        #result = await manager_proxy['add_job_to_queue'](
        #    queue, message
        #)
        args = message.get('args') if 'args' in message else []
        kwargs = message.get('kwargs') if 'kwargs' in message else {}

        result = await manager_proxy[message['name']](*args, **kwargs)

        print(f"publish result: {result}")
    print(f"producer exiting")

async def get_producer_channel(
    manager_host: str,
    manager_port: str,
    manager_path: str,
    manager_secret: str,
    queue: str
):
    """
    returns Channel object with an intialized producer for adding jobs to a Job Manager Queue
        
    channel.send_job(job)
        sends a single job to queue in job manager endpoint

    channel.send_jobs([job, job1, job])
        sends a list of jobs to queue in job manager endpoint

    jobs should be a dictonary with the following options:
    
    job
        {
            'job': {
                'name': 'name of function registered'
                'args': [1,2, 3] Optional otherwise assumed empty as []
                'kwargs': {'a': 1, 'b': 2, 'c': 3} Optional, otherwise assumed empty as {}
            }
        }
    """
    channel =  await Channel.create(
        producer(
            manager_host,
            manager_port,
            manager_path,
            manager_secret,
            queue
        )
    )
    return channel

async def test():
    channel = await get_producer_channel(
        '192.168.1.18',
        '8220',
        '/ws/jobs',
        manager_secret='abcd1234',
        queue='DEFAULT'
    )
    for i in range(10):
        await channel.send_job(
            {
                'name': 'worker_a',
                'args': [i+1, i+2, i+3]
            }
        )
    del channel


if __name__ == '__main__':
    asyncio.run(test())