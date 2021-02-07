import asyncio
import aio_pika

async def rabbitmq_message_generator(
    manager,
    rabbit_mq_path: str,
    queue_name
):   
    log = manager.log
    connection = await aio_pika.connect_robust(rabbit_mq_path)

    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        while True:
            try:
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            print(message.body)
                            status = yield message.body.decode()
                            if status == 'finished':
                                raise asyncio.CancelledError(f"rabbitmq finished")
                            #if queue.name in message.body.decode():
                            #    break
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                log.exception(f"rabbitmq - error pulling messages")
                
async def message_consumer(manager, queue):
    manager.log.warning(f"message_consumer - starting - for type {manager.broker_type} ")
    message_generator = manager.message_generator(manager, manager.broker_path, queue)
    while True:
        try:
            job = await message_generator.asend(None)
            if job and 'job' in job:
                manager.log.debug(f"message_consumer pulled {job}")
                job = json.loads(job)['job']
                manager.log.debug(f"message_consumer - job: {job}")

                job['namespace'] = queue
                if not 'args' in job:
                    job['args'] = {'args': []}
                else:
                    job['args'] = {'args': job['args']}
                if not 'kwargs' in job:
                    job['kwargs'] = {}
                result = await manager.run_job(
                    queue, job['name'], args=job['args']['args'], kwargs=job['kwargs']
                )
                manager.log.debug(f"message_consumer run_job result: {result}")
        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                break
            manager.log.exception(f"ERROR: message_consumer failed to pull messages from broker {manager.broker_type}")
            await asyncio.sleep(5)
            message_generator = manager.message_generator(manager, manager.broker_path, queue)

    await message_generator.asend('finished')