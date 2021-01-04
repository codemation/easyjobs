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