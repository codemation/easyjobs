import asyncio
import uuid, json, time
import logging
import random

from easyrpc.server import EasyRpcServer
from easyrpc.register import Coroutine
from fastapi import FastAPI
from aiopyql import data


async def database_setup(server, db):
    if not 'jobs' in db.tables:
        await db.create_table(
            'jobs',
            [
                ('job_id', str, 'UNIQUE'),
                ('namespace', str),
                ('node_id', str),
                ('status', str),
                ('name', str),
                ('args', str),
                ('kwargs', str),
                ('retry_policy', str),
                ('on_failure', str),
                ('run_after', str)
            ],
            'job_id',
            cache_enabled=True
        )
    if not 'results' in db.tables:
        await db.create_table(
            'results',
            [
                ('job_id', str, 'UNIQUE'),
                ('results', str)
            ],
            'job_id',
            cache_enabled=True
        )
    @server.on_event('shutdown')
    async def shutdown():
        self.log.debug(f"closing db {db}")
        await db.close()

class EasyJobsManager():
    def __init__(
        self,
        rpc_server: EasyRpcServer,
        database: data.Database,
        broker_type: str = None,
        broker_path: str = None
    ):
        self.rpc_server = rpc_server
        self.log = self.rpc_server.log

        self.job_queues = {}
        self.job_results = {}
        self.db = database

        self.workers = []
        self.worker_instances = set()
        self.create_cron_task(
            self.refresh_worker_pool, 60
        )

        self.broker_type = broker_type
        self.broker_path = broker_path
        self.BROKER_TYPES = {'rabbitmq'}

        self.new_job_queue = asyncio.Queue()

        self.tasks = {}
        
    @classmethod
    async def create(
        cls,
        server: FastAPI,
        jobs_path: str, # path accessed to start WS connection /ws/my_origin_paths
        server_secret: str, 
        broker_type: str = None,
        broker_path: str = None,
        encryption_enabled: bool = False,
        logger: logging.Logger = None,
        debug: bool = False
    ):

        rpc_server = await EasyRpcServer.create(
            server,
            jobs_path, # path accessed to start WS connection /ws/my_origin_paths
            server_secret,
            encryption_enabled,
            logger=logger,
            debug=debug,
        )

        database = await data.Database.create(
            database='job_manager.db',
            log=logger,
            cache_enabled=True
        )
        # trigger table creation - if needed
        await database_setup(server, database)

        job_manager = cls(
            rpc_server,
            database,
            broker_type,
            broker_path
        )
        # load existing jobs before returning
        await job_manager.broker_setup()
        await job_manager.load_job_queues()

        @rpc_server.origin(namespace='manager')
        async def add_job_to_queue(queue: str, job: dict):
            """
            expects job with following parameters:
            {
                'namespace': 'name'
                'name': 'name',
                'args': [args],
                'kwargs': {'kwarg': 'val'}
            }
            """
            if 'job' in job:
                job = job['job']
                job['namespace'] = queue

                if not 'args' in job:
                    job['args'] = {'args': []}
                else:
                    job['args'] = {'args': job['args']}
                if not 'kwargs' in job:
                    job['kwargs'] = {}

            return await job_manager.add_job(job)
        
        @rpc_server.origin(namespace='manager')
        async def add_task(queue, task_name):
            return await job_manager.add_task(queue, task_name)

        @rpc_server.origin(namespace='manager')
        async def get_job_from_queue(queue):
            return await job_manager.get_job_from_queue_nowait(queue)
            
        @rpc_server.origin(namespace='manager')
        async def get_job_result(job_id):
            return await job_manager.get_job_result(job_id)
        
        @rpc_server.origin(namespace='manager')
        async def add_job_queue(queue):
            return await job_manager.add_job_queue(queue)

        @rpc_server.origin(namespace='manager')
        async def add_job_results(job_id, results):
            return await job_manager.add_job_results(job_id, results)

        @rpc_server.origin(namespace='manager')
        async def update_job_status(job_id, status, node_id):
            return await job_manager.update_job_status(job_id, status, node_id)

        @rpc_server.origin(namespace='manager')
        async def run_job(queue, name, args = None, kwargs = None):
            return await job_manager.run_job(queue, name, args, kwargs)

        @rpc_server.origin(namespace='manager')
        async def requeue_job(queue, job):
            return await job_manager.requeue_job(queue, job)

        @rpc_server.origin(namespace='manager')
        async def delete_job(job_id):
            return await job_manager.delete_job(job_id)

        @rpc_server.origin(namespace='manager')
        async def add_worker_to_pool(worker_id):
            return await job_manager.add_worker_to_pool(worker_id)
        
        return job_manager
    def create_cron_task(self, task, interval):
        async def cron():
            while True:
                try:
                    results = task()
                    if isinstance(results, Coroutine):
                        results = await results
                except Exception as e:
                    if isinstance(e, asyncio.CancelledError):
                        break
                    self.log.exception(f"error with cron task {task.__name__}")
                await asyncio.sleep(interval)
        self.workers.append(
            asyncio.create_task(
                cron()
            )
        )
    
    async def add_worker_to_pool(self, worker_id):
        """
        adds a worker_id to self.worker_instances
        """
        if not worker_id in self.worker_instances:
            self.worker_instances.add(worker_id)
    async def refresh_worker_pool(self):
        for worker_id in self.worker_instances.copy():
            if not worker_id in self.rpc_server.reverse_proxies:
                self.log.warning(f"{worker_id} not found in {self.rpc_server.reverse_proxies}")
                self.worker_instances.remove(worker_id)
            for namespace in self.rpc_server.server_proxies:
                if worker_id in self.rpc_server.server_proxies[namespace]:
                    self.log.debug(f"reverse - proxy_funcs {self.rpc_server.server_proxies[namespace][worker_id].proxy_funcs}")
        self.log.warning(f"refresh_worker_pool - {self.worker_instances}")
        return list(self.worker_instances)

    async def broker_setup(self):
        """
        creates broker connection and worker for 
        consuming new messages
        """
        if self.broker_type == 'rabbitmq':
            from easyjobs.brokers.rabbitmq import rabbitmq_message_generator
            self.message_generator = rabbitmq_message_generator

    async def start_message_consumer(self, queue):
        generator = self.message_generator(self, self.broker_path, queue)
        self.workers.append(
            asyncio.create_task(
                self.message_consumer(queue, generator)
            )
        )

    async def message_consumer(self, queue, message_generator):
        self.log.warning(f"message_consumer - starting - for type {self.broker_type} ")
        
        while True:
            try:
                job = await message_generator.asend(None)
                if job and 'job' in job:
                    job = json.loads(job)['job']
                    self.log.debug(f"message_consumer - job: {job}")

                    job['namespace'] = queue
                    if not 'args' in job:
                        job['args'] = {'args': []}
                    else:
                        job['args'] = {'args': job['args']}
                    if not 'kwargs' in job:
                        job['kwargs'] = {}
                    result = await self.run_job(
                        queue, job['name'], args=job['args']['args'], kwargs=job['kwargs']
                    )
                    self.log.warning(f"message_consumer run_job result: {result}")
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"message_consumer exception")
        await message_generator.asend('finished')
    
    def get_local_worker_task(self, queue, task_name, task_type):
        worker_id = '_'.join(self.rpc_server.server_id.split('-'))
        local_funcs = self.rpc_server[f'local_{queue}']
        self.log.warning(f"get_local_worker_task - local_funcs: {local_funcs}")
        return local_funcs.get(f'{task_name}_{worker_id}_{task_type}')


    def get_random_worker_task(self, queue, task_name, task_type):
        global_funcs = self.rpc_server[f'{queue}']
        worker_funcs = []
        for func in global_funcs:
            if task_name in func and task_type in func:
                worker_funcs.append(global_funcs[func])
        if worker_funcs:
            return random.choice(worker_funcs)
        return worker_funcs

    async def add_task(self, queue: str, task_name: str):
        """
        called by workers to ensure task load balancer exists for 
        registered task
        
        if not queue in self.tasks:
            self.tasks[queue] = {}
        if not task in self.tasks[queue]:
        """
        async def task(*args, **kwargs):
            job = self.get_random_worker_task(queue, task_name, 'job')
            job_id = await job(*args, **kwargs)
            return job_id
        task.__name__ = task_name
        self.rpc_server.origin(task, namespace=queue)
        return f"{task_name} registered"

    def task(
        self, 
        namespace: str = 'DEFAULT',
        on_failure: str = None, # on failure job
        retry_policy: str =  'retry_once', # retry_once, retry_always, never
        run_after: str = None
    ):

        worker_id = '_'.join(self.rpc_server.server_id.split('-'))

        def job_register(func):
            self.rpc_server.origin(func, namespace=f'local_{namespace}')

            func_name = f"{func.__name__}"

            async def job(*args, **kwargs):
                job_id = str(uuid.uuid1())
                new_job = {
                    'job_id': job_id,
                    'namespace': namespace,
                    'name': func_name,
                    'args': {'args': list(args)},
                    'kwargs': kwargs,
                    'retry_policy': retry_policy,
                    'on_failure': on_failure,
                    'run_after': run_after
                }
                self.log.warning(f"new_job: {new_job}")
                await self.new_job_queue.put(new_job)
                return job_id
            
            async def task(*args, **kwargs):
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, Coroutine):
                        result = await result
                    return result
                except Exception as e:
                    if not isinstance(e, asyncio.CancelledError):
                        self.log.exception(f"exception running {func_name}")
                        return f'task {func_name} failed'

            job.__name__ = f'{func_name}_{worker_id}_job'
            
            self.rpc_server.origin(job, namespace=namespace)

            task.__name__ = f"{func_name}_{worker_id}_task"
            self.rpc_server.origin(task, namespace=namespace)

            asyncio.create_task(self.add_task(namespace, func_name))

            return job
        return job_register

    async def add_job_queue(self, queue: str):
        if not queue in self.job_queues:
            self.job_queues[queue] = asyncio.Queue()
            await self.start_queue_workers(queue)
            if self.broker_type and self.broker_type in self.BROKER_TYPES:
                await self.start_message_consumer(queue)
    
    async def load_job_queues(self):
        # start job_sender
        self.workers.append(
            asyncio.create_task(self.job_sender())
        )

        jobs = await self.db.tables['jobs'].select('*')
        for job in jobs:
            queue = job['namespace']
            if not queue in self.job_queues:
                await self.add_job_queue(queue)
            await self.job_queues[queue].put(job)
            self.job_results[job['job_id']] = asyncio.Queue()

    async def start_queue_workers(self, queue: str, count: int = 1):
        for _ in range(count):
            self.workers.append(
                asyncio.create_task(
                    self.worker(queue)
                )
            )

    
    async def get_job_from_queue(self, queue):
        return await self.job_queues[queue].get()
    async def get_job_from_queue_nowait(self, queue):
        try:
            return self.job_queues[queue].get_nowait()
        except asyncio.QueueEmpty:
            return {"queue_empty": {}}

    async def update_job_status(self, job_id: str, status: str, node_id: str = None):
        await self.db.tables['jobs'].update(
            status=status,
            node_id=node_id,
            where={'job_id': job_id}
        )
    async def requeue_job(self, queue, job):
        """
        places existing job back into global queue
        typical in retry_always tasks
        """
        await self.job_queues[queue].put(job)

    async def delete_job(self, job_id):
        await self.db.tables['jobs'].delete(
            where={'job_id': job_id}
        )

    async def run_job(self, queue, name, args = None, kwargs = None):
        if not args:
            args = []
        if not kwargs:
            kwargs = {}
        self.log.debug(f"run_job called for {name} in queue {queue} with args: {args} kwargs {kwargs}")
        job = self.get_random_worker_task(queue, name, 'job')
        if not type(job) is list:
            return await job(*args, **kwargs)
        raise Exception(f"no job found with task name {name} in queue {queue}")

    async def run_task(self, queue, name, args, kwargs):
        # attempt to run local version first - if any
        results = None
        local_funcs = self.rpc_server[f'local_{queue}']
        global_func = self.get_random_worker_task(queue, name, 'task')
        if name in local_funcs:
            self.log.debug(f"running local func for task")
            results = local_funcs[name](*args['args'], **kwargs)
        elif not type(global_func) == list:
            self.log.debug(f"running global func for task")
            results = global_func(*args['args'], **kwargs)
        else:
            self.log.error(f"no task found with name: {name} - adding job back to queue")
            return f"task {name} failed"

        if isinstance(results, Coroutine):
            results = await results
        self.log.debug(f"run_task results: {results}")
        return results

    async def job_sender(self):
        self.log.warning(f"starting job_sender")
        while True:
            try:
                job = await self.new_job_queue.get()
                queue = job['namespace']
                await self.add_job(job)
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"error with job_sender")
                
        self.log.warning(f"job_sender exiting")

    async def worker(self, queue):
        self.log.warning(f"worker started")
        while True:
            try:
                #job = await self.job_queues[queue].get()
                job = await self.get_job_from_queue(queue)
                job_id = job['job_id']
                
                self.log.debug(f"worker pulled {job} from queue")

                # start job
                name, args, kwargs = job['name'], job['args'], job['kwargs']

                results = None
                for _ in range(2):
                    results = await self.run_task(queue, name, args, kwargs)

                    if results == f'task {name} failed':
                        if job['retry_policy'] == 'never':
                            break
                        continue

                if results == f'task {name} failed':
                    if job['on_failure']: 
                        try:
                            await self.run_job(queue, job['on_failure'], kwargs={'job_failed': results})
                        except Exception as e:
                            self.log.exception(f"error running on_failure task {job['on_failure']} triggered by {name} failure")

                    if job['retry_policy'] == 'retry_always':
                        await self.update_job_status(
                            job_id, 'queued', None
                        )
                        await self.requeue_job(queue, job)
                        continue

                # mark running
                await self.update_job_status(
                    job_id, 'running', self.rpc_server.server_id
                )

                # update results
                await self.add_job_results(job_id, results)

                # delete job
                await self.delete_job(job_id)

                if job['run_after'] and not results == f'task {name} failed':
                    await self.run_job(queue, job['run_after'], kwargs=results)
                
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"error in worker")
        self.log.warning(f"worker exiting")
    async def add_job(self, job: dict):
        """
        jobs consist of the following:
        job = {
            'job_id': 'uuid'
            'namespace': 'name'
            'status': 'queued | running '
            'name': 'name',
            'args': [args],
            'kwargs': {'kwargs': 'vals'}
        }
        """
        job_id = str(uuid.uuid1()) if not 'job_id' in job else job['job_id']

        new_job = {
            'status': 'queued'
        }
        new_job.update(job)


        await self.db.tables['jobs'].insert(
            status='queued',
            **job
        )
        namespace = new_job['namespace']

        if not namespace in self.job_queues:
            await self.add_job_queue(namespace)
        
        await self.job_queues[namespace].put(new_job)

        self.job_results[job_id] = asyncio.Queue()
        return job_id

    async def add_job_results(self, job_id: str, results: dict):
        add_results = await self.db.tables['results'].insert(
            job_id=job_id,
            results=results
        )
        await self.job_results[job_id].put(results)
    
    async def get_job_result(self, job_id):
        start = time.time()
        while time.time() - start < 5.0:
            if not job_id in self.job_results:
                await asyncio.sleep(time.time() - start)
                continue
            break

        if not job_id in self.job_results:
            raise Exception(f"no job startd with id {job_id}")

        result = await self.job_results[job_id].get()
        await self.db.tables['results'].delete(where={'job_id': job_id})
        return result