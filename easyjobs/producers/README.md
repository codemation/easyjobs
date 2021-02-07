# Producers
Create Jobs to be run

## Rabbit MQ  - Send Jobs to Rabbit MQ


    import asyncio
    from easyjobs.producers.rabbitmq import get_producer_channel

    async def main():
        channel = await get_producer_channel(
            'amqp://guest:guest@127.0.0.1/',
            'DEFAULT'
        )
        for i in range(10):
            await channel.send_job(
                {
                    'job': {
                        'name': 'basic_job',
                        'args': [i+1, i+2, i+3]
                    }
                }
            )
        del channel

    asyncio.run()