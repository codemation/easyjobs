import asyncio

async def easyjobs_message_generator(
    manager,
    queue_name,
):
    log = manager.log

    while True:
        try:
            queued = await manager.db.tables['job_queue'].select(
                '*', where={'namespace': queue_name}
            )
            if len(queued) == 0:
                status = yield 'empty'
                if status == 'finished':
                    raise asyncio.CancelledError(f"rabbitmq finished")
                continue
            for job in queued:

                status = yield job
                if status == 'finished':
                    raise asyncio.CancelledError(f"rabbitmq finished")

                if status == 'added':
                    await manager.db.tables['job_queue'].delete(
                        where={'request_id': job['request_id']}
                    )
                status = yield
                if status == 'finished':
                    raise asyncio.CancelledError(f"rabbitmq finished")

        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                break
            log.exception(f"rabbitmq - error pulling messages")

async def message_consumer(manager, queue):
    manager.log.warning(f"easyjobs - message_consumer - starting ")
    message_generator = manager.message_generator(manager, queue)
    while True:
        try:
            job = await message_generator.asend(None)
            if job == 'empty':
                manager.log.warning(f"easyjobs queue {queue} empty, sleeping(5)")
                await asyncio.sleep(5)
            
            if job and 'job' in job:
                job = job['job']
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
                manager.log.warning(f"add_job result: {result}")
                # notify broker that job was added
                await message_generator.asend('added')
        
        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                break
            manager.log.exception(f"error in easyjobs broker")