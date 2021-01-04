import json
import asyncio
import aio_pika

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
        if not 'job' in job:
            raise Exception(f"no job key provided in message {job}")
        await self.channel_generator.asend(job)
        return
    async def send_jobs(self, job_list: list):
        for job in job_list:
            await self.send_job(job)
        return
    def __del__(self):
        asyncio.create_task(self.channel_generator.asend('finished'))

async def producer(rabbit_mq_path, queue):
    connection = await aio_pika.connect_robust(
        rabbit_mq_path # amqp://guest:guest@127.0.0.1/"
    )
    async with connection:

        channel = await connection.channel()
        message = yield 'starting'

        if message == 'finished':
            yield 'finished'
            
        while True:

            print(f"producer waiting for messages")
            message = yield
            if message == 'finished':
                break
            print(f"producer received message: {message}")
            result = await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(message).encode()),
                routing_key=queue,
            )
            print(f"publish result: {result}")
    print(f"producer exiting")

async def get_producer_channel(rabbit_mq_path, queue):
    """
    returns Channel object with an intialized producer for adding jobs to a RabbitMQ Queue
        
    channel.send_job(job)
        sends a single job to queue in rabbitmq endpoint

    channel.send_jobs([job, job1, job])
        sends a list of jobs to queue in rabbitmq endpoint

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
    channel =  await Channel.create(producer(rabbit_mq_path, queue))
    return channel

async def test():
    channel = await get_producer_channel(
        'amqp://guest:guest@127.0.0.1/',
        'DEFAULT'
    )
    for i in range(20):
        await channel.send_job(
            {
                'job': {
                    'name': 'worker_a',
                    'args': [i+1, i+2, i+3]
                }
            }
        )
    del channel


if __name__ == '__main__':
    asyncio.run(test())

