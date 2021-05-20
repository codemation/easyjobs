import asyncio
import uuid, json, time, os, datetime
import logging
import random
import subprocess as sp
from typing import Optional, Callable
from easyrpc.server import EasyRpcServer
from easyrpc.register import Coroutine, create_proxy_from_config, get_signature_as_dict
from fastapi import FastAPI
from aiopyql import data
from easyjobs.api.manager_api import api_setup
from easyschedule import EasyScheduler


async def database_setup(job_manager, server, db):
    await db.create_table(
        'job_queue',
        [
            ('request_id', str, 'UNIQUE'),
            ('namespace', str),
            ('job', str),
            ('job_id', str)
        ],
        'request_id',
        cache_enabled=True
    )
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
            ('run_before', str),
            ('run_after', str)
        ],
        'job_id',
        cache_enabled=True
    )
    await db.create_table(
        'results',
        [
            ('job_id', str, 'UNIQUE'),
            ('results', str)
        ],
        'job_id',
        cache_enabled=True
    )
    
    
class EasyJobsManager():
    def __init__(
        self,
        rpc_server: EasyRpcServer,
        database: data.Database,
        scheduler: EasyScheduler,
        broker_type: str = None,
        broker_path: str = None,
        api_router = None
    ):
        self.rpc_server = rpc_server
        self.rpc_server.server.title = f"Easyjobs Manager"
        self.log = self.rpc_server.log
        if api_router:
            self.api_router = api_router
        else:
            self.api_router = self.rpc_server.server

        self.job_queues = {}
        self.job_results = {}
        self.db = database
        self.scheduler = scheduler

        self.workers = []
        self.pause_workers = False
        self.worker_instances = set()
        self.create_cron_task(
            self.refresh_worker_pool, 60
        )

        self.broker_type = broker_type
        self.broker_path = broker_path
        self.BROKER_TYPES = {'rabbitmq'}

        self.new_job_queue = asyncio.Queue()

        self.tasks = {}
        self.task_subprocess_results = {}
        self.task_results = {}

        self.callback_results = {}
        self.worker_callbacks = {}
        
    @classmethod
    async def create(
        cls,
        server: FastAPI,
        server_secret: str, 
        broker_type: str = None,
        broker_path: str = None,
        api_router = None,
        encryption_enabled: bool = False,
        logger: logging.Logger = None,
        debug: bool = False
    ):

        rpc_server = await EasyRpcServer.create(
            server,
            '/ws/jobs', # path accessed to start WS connection /ws/my_origin_paths
            server_secret,
            encryption_enabled,
            logger=logger,
            debug=debug,
        )
        log = rpc_server.log
        log.debug(f"JOB_MANAGER SETUP: rpc_server created")

        db_path = 'job_manager.db' if not 'DB_PATH' in os.environ else f"{os.environ['DB_PATH']}/job_manager.db"

        database = await data.Database.create(
            database=db_path,
            log=rpc_server.log,
            cache_enabled=True,
            debug=debug
        )
        # trigger table creation - if needed
        log.debug(f"JOB_MANAGER SETUP: database created")
        
        scheduler = EasyScheduler(logger=rpc_server.log)

        job_manager = cls(
            rpc_server,
            database,
            scheduler,
            broker_type,
            broker_path,
            api_router
        )
        log.debug(f"JOB_MANAGER SETUP: job_manager created")

        await database_setup(job_manager, server, database)

        @server.on_event('shutdown')
        async def shutdown():
            await job_manager.close()

        log.debug(f"JOB_MANAGER SETUP: job_manager setup 1 completed")

        # load existing jobs/results before returning
        await job_manager.broker_setup()
        log.debug(f"JOB_MANAGER SETUP: job_manager setup 2 completed")

        await job_manager.load_job_queues()
        log.debug(f"JOB_MANAGER SETUP: job_manager setup 3 completed")

        await job_manager.load_results_queue()
        log.debug(f"JOB_MANAGER SETUP: job_manager setup 4 completed")

        await job_manager.setup_scheduler()
        log.debug(f"JOB_MANAGER SETUP: job_manager setup 5 completed")

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
                for list_item in {'run_before', 'run_after'}:
                    if not job[list_item]:
                        job[list_item] = None
                    else:
                        job[list_item] = {list_item: job[list_item]}


            return await job_manager.add_job(job)
        
        @rpc_server.origin(namespace='manager')
        async def add_task(queue, task_name, sig_config, schedule, default_args):
            return await job_manager.add_task(queue, task_name, sig_config, schedule, default_args)

        @rpc_server.origin(namespace='manager')
        async def create_task_subprocess_callback(request_id, worker_id):
            return await job_manager.create_task_subprocess_callback(request_id, worker_id)

        @rpc_server.origin(namespace='manager')
        async def create_job_result_callback(job_id, worker_id):
            return await job_manager.create_job_result_callback(job_id, worker_id)
        
        @rpc_server.origin(namespace='manager')
        async def add_callback_results(request_id, results):
            return await job_manager.add_callback_results(request_id, results)

        @rpc_server.origin(namespace='manager')
        async def get_job_from_queue(queue):
            return await job_manager.get_job_from_queue_nowait(queue)
        
        @rpc_server.origin(namespace='manager')
        async def create_get_job_from_queue_callback(queue, request_id, worker_id):
            return await job_manager.create_get_job_from_queue_callback(
                queue, request_id, worker_id
            )
            
        @rpc_server.origin(namespace='job_results')
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

        @job_manager.task(namespace='job_manager')
        async def jobmanager_started():
            message = f"JOB MANAGER started - {datetime.datetime.now().isoformat()}"
            job_manager.log.warning(message)
            return message

        log.debug(f"JOB_MANAGER SETUP: job_manager setup 5 completed")

        await jobmanager_started()

        log.debug(f"JOB_MANAGER SETUP: job_manager setup 6 completed - finished setup")
        
        await api_setup(job_manager)

        return job_manager
    def __del__(self):
        self.log.warning(f"JOB_MANAGER is closing")
        asyncio.create_task(self.close())
    async def close(self):
        """
        stops running running process task
        """
        
        self.log.debug(f"CLOSING: workers: {self.workers}")
        for worker in self.workers:
            worker.cancel()
        
        await self.db.close()
        self.log.warning(f"JOB_MANAGER successfully closed")
        
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
    async def setup_scheduler(self):

        # start scheduler
        self.workers.append(
            asyncio.create_task(self.scheduler.start())
        )
        self.log.warning(f"JOBS_MANAGER - scheduler started")
    
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
                if worker_id in self.worker_callbacks:
                    for callback in self.worker_callbacks[worker_id]:
                        callback.cancel()
                    del self.worker_callbacks[worker_id]
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
        self.log.debug(f"JOB_MANAGER broker_setup started")
        if self.broker_type == 'rabbitmq':
            from easyjobs.brokers.rabbitmq import rabbitmq_message_generator, message_consumer
            self.message_generator = rabbitmq_message_generator
            self.message_consumer = message_consumer
        else:
            from easyjobs.brokers.easyjobs import easyjobs_message_generator, message_consumer
            self.message_generator = easyjobs_message_generator
            self.message_consumer = message_consumer

        self.log.debug(f"JOB_MANAGER broker_setup completed")

    async def start_message_consumer(self, queue):
        self.workers.append(
            asyncio.create_task(
                self.message_consumer(self, queue)
            )
        )
    
    def get_local_worker_task(self, queue, task_name, task_type):
        worker_id = '_'.join(self.rpc_server.server_id.split('-'))
        try:
            local_funcs = self.rpc_server[f'local_{queue}']
            self.log.debug(f"get_local_worker_task - local_funcs: {local_funcs}")
        except IndexError:
            return None
        return local_funcs.get(f'{task_type}_{task_name}_{task_type}_{worker_id}')

    def get_random_worker_task(self, queue, task_name, task_type):
        
        worker_funcs = []
        try:
            global_funcs = self.rpc_server[f'{queue}']
            for func in global_funcs:
                if f'{task_type}_{task_name}_{task_type}' in func:
                    worker_funcs.append(global_funcs[func])
            if worker_funcs:
                return random.choice(worker_funcs)
            return worker_funcs
        except IndexError:
            return worker_funcs

    async def add_task(
        self, 
        queue: str, 
        task_name: str, 
        sig_config: dict,
        schedule: str,
        default_args: dict
    ):
        """
        called by workers to ensure task load balancer exists for 
        registered task
        
        if not queue in self.tasks:
            self.tasks[queue] = {}
        if not task in self.tasks[queue]:
        """
        await self.add_job_queue(queue)

        async def task(*args, **kwargs):
            request_id = str(uuid.uuid1())
            await self.db.tables['job_queue'].insert(
                request_id=request_id,
                namespace=queue,
                job={
                    'name': task_name,
                    'args': args,
                    'kwargs': kwargs
                }
            )
            return {'request_id': request_id}

        task_proxy = create_proxy_from_config(sig_config, task)

        task_proxy.__name__ = task_name

        if not queue == 'job_manager':
            # removing current openapi schema to allow refresh
            self.rpc_server.server.openapi_schema = None
            self.api_router.post(f'/{queue}/task/{task_name}', tags=[queue])(task_proxy)
            if not schedule is None:
                if not task_proxy.__name__ in self.scheduler.scheduled_tasks:
                    self.scheduler.schedule(
                        task_proxy,
                        schedule=schedule,
                        default_args=default_args
                    )
        self.log.warning(f"add_task - default_args: {default_args}")

        self.rpc_server.origin(task_proxy, namespace=queue)
        return f"{task_name} registered"

    def task(
        self, 
        namespace: str = 'DEFAULT',
        on_failure: Optional[str] = None, # on failure job
        retry_policy: Optional[str] =  'retry_once', # retry_once, retry_always, never
        run_before: Optional[list] = None,
        run_after: Optional[list] = None,
        subprocess: Optional[bool] = False,
        schedule: Optional[str] = None,
        default_args: Optional[dict] = None
    ) -> Callable:

        worker_id = '_'.join(self.rpc_server.server_id.split('-'))

        if default_args:
            if not 'args' in default_args:
                default_args['args'] = []
            if not 'kwargs' in default_args:
                default_args['kwargs'] = {}
        

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
                    'run_before': run_before,
                    'run_after': run_after
                }
                await self.new_job_queue.put(new_job)
                return job_id

            if subprocess:
                REQUIRED_ENV_VARS = {'MANAGER_HOST', 'MANAGER_PORT', 'WORKER_TASK_DIR'}
                for var in REQUIRED_ENV_VARS:
                    if os.environ.get(var) is None:
                        raise Exception(f"missing env var {var} - required to use ")

                MANAGER_HOST = os.environ.get('MANAGER_HOST')
                WORKER_TASK_DIR = os.environ.get('WORKER_TASK_DIR')
                MANAGER_PORT = os.environ.get('MANAGER_PORT')

                if not os.system(f"ls {WORKER_TASK_DIR}/{func_name}.py") == 0:
                    template_location = 'https://github.com/codemation/easyjobs/tree/main/easyjobs/workers'
                    raise Exception(
                        f"missing expected {WORKER_TASK_DIR}/{func_name}.py task_subprocess file, create using template located here: {template_location}"
                    )
                async def task(*args, **kwargs):
                    request_id = str(uuid.uuid1())
                    self.log.debug(f"task started - subprocess - for {func_name}")
                    #self.task_subprocess_results[request_id] = asyncio.Queue()
                    self.callback_results[request_id] = asyncio.Queue()
 
                    # start task_subprocess
                    arguments = json.dumps({'args': list(args), 'kwargs': kwargs})
                    p = sp.Popen(
                        [
                            'python', 
                            f'{WORKER_TASK_DIR}/{func_name}.py',
                            MANAGER_HOST,
                            MANAGER_PORT,
                            self.rpc_server.origin_path,
                            self.rpc_server.server_secret,
                            request_id,
                            arguments
                        ]
                    )
                    # create_task_subprocess_callback - use 
                    try:
                        return await self.callback(request_id)
                    except Exception as e:
                        if not isinstance(e, asyncio.CancelledError):
                            return f'task {func_name} failed'
            else:
                async def task(*args, **kwargs):
                    self.log.debug(f"task started - normal - for {func_name}")
                    try:
                        result = func(*args, **kwargs)
                        if isinstance(result, Coroutine):
                            result = await result
                        return result
                    except Exception as e:
                        if not isinstance(e, asyncio.CancelledError):
                            self.log.exception(f"exception running {func_name}")
                            return f'task {func_name} failed'

            job.__name__ = f'job_{func_name}_job_{worker_id}'
            
            self.rpc_server.origin(job, namespace=namespace)

            task.__name__ = f"task_{func_name}_task_{worker_id}"
            self.rpc_server.origin(task, namespace=namespace)
            self.rpc_server.origin(task, namespace=f'local_{namespace}')

            sig_config = {'name': func_name, 'sig': get_signature_as_dict(func)}

            asyncio.create_task(self.add_task(namespace, func_name, sig_config, schedule, default_args))

            return job
        return job_register

    async def add_job_queue(self, queue: str):
        if not queue in self.job_queues:
            #distributor = self.job_queue_distributor(queue)
            #await distributor.asend(None)
            #self.job_queues[queue] = distributor
            self.job_queues[queue] = 'working'
            await self.start_queue_workers(queue)
            #if self.broker_type and self.broker_type in self.BROKER_TYPES:
            await self.start_message_consumer(queue)
    
    async def load_job_queues(self):
        self.log.debug(f"JOB_MANAGER load_job_queues started")
        # start job_sender
        self.workers.append(
            asyncio.create_task(self.job_sender())
        )

        jobs = await self.db.tables['jobs'].select('*')
        for job in jobs:
            queue = job['namespace']
            if not queue in self.job_queues:
                await self.db.tables['jobs'].update(
                    node_id=None,
                    where={
                        'status': 'queued'
                    }
                )
                await self.add_job_queue(queue)
            self.job_results[job['job_id']] = asyncio.Queue()
        self.log.debug(f"JOB_MANAGER load_job_queues finished")

    async def load_results_queue(self):
        self.log.debug(f"JOB_MANAGER load_results_queue started")
        results = await self.db.tables['results'].select('*')
        for result in results:
            if not result['job_id'] in self.job_results:
                self.job_results[result['job_id']] = asyncio.Queue()
                await self.job_results[result['job_id']].put(result['results'])
        self.log.debug(f"JOB_MANAGER load_results_queue finished")

    async def start_queue_workers(self, queue: str, count: int = 1):
        for _ in range(count):
            self.workers.append(
                asyncio.create_task(
                    self.worker(queue)
                )
            )
    async def create_get_job_from_queue_callback(self, queue, request_id, worker_id):
        if not queue in self.job_queues:
            await self.add_job_queue(queue)
        get_job_from_queue = self.get_job_from_queue(queue, worker_id, request_id)
        await self.create_callback(get_job_from_queue, request_id, worker_id)
        return f"get_job_from_queue callback"

    async def get_job_from_queue(self, queue, worker_id, request_id):
        while True:
            try:
                job = None
                async for jb in self.job_queue_distributor(queue):
                    if jb == 'empty':
                        #self.log.warning(f"get_job_from_queue - {jb}")
                        await asyncio.sleep(5)
                        continue
                    job=jb
                    break

                # reserve job 
                await self.db.tables['jobs'].update(
                    node_id=f"{worker_id}-REQ-{request_id}",
                    status='pulled',
                    where={
                        'job_id': job['job_id'],
                        'status': 'queued'
                    }
                )

                # verify reservation
                job = await self.db.tables['jobs'].select(
                    '*', 
                    where={'job_id': job['job_id']}
                )

                # return if reserved
                if job[0]['node_id'] == f"{worker_id}-REQ-{request_id}":
                    return job[0]
                
                # failed to reserve job
                self.log.warning(f"failed to reserve job - {job}")
                await asyncio.sleep(1)

            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"get_job_from_queue - error")
                #break

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
        job = self.get_random_worker_task(queue, name, 'job')
        if not type(job) is list:
            return await job(*args, **kwargs)
        raise Exception(f"no job found with task name {name} in queue {queue}")

    async def run_task(self, queue, name, args, kwargs):
        # attempt to run local version first - if any
        results = None
        local_func = self.get_local_worker_task(queue, name, 'task')
        
        if not local_func is None:
            self.log.debug(f"run_task - using local_func {local_func}")
            results = local_func(*args['args'], **kwargs)
        else:
            global_func = self.get_random_worker_task(queue, name, 'task')
            self.log.debug(f"run_task - using global_func {global_func}")
            if not type(global_func) == list:
                results = global_func(*args['args'], **kwargs)

        if results is None:
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
        return

    def toggle_workers(self, pause: bool):
        """
        Enables or disables workers ability to pull new jobs
        ::pause = False 
        """
        self.log.warning(f"toggle_workers triggered - pause set to {pause}")
        self.pause_workers = pause

    async def worker(self, queue):
        self.log.warning(f"worker started")
        while True:
            try:
                if self.pause_workers:
                    await asyncio.sleep(5)
                    continue
                request_id = str(uuid.uuid1())
                job = await self.get_job_from_queue(
                    queue, 
                    self.rpc_server.server_id,
                    request_id
                )
                #breakpoint()
                job_id = job['job_id']
                
                self.log.debug(f"worker pulled {job} from queue")
    
                if job['run_before']:
                    tasks = []
                    for task in job['run_before']:
                        if task in self.rpc_server[queue]:
                            before_job_id = await self.run_job(queue, task)
                            self.log.debug(f"RUN_BEFORE: request_id - {before_job_id}")
                            tasks.append(before_job_id)
                    await self.update_job_status(
                        job_id, 'waiting', self.rpc_server.server_id
                    )
                    for before_job_id in tasks:
                        result = await self.get_job_result(before_job_id)
                        self.log.debug(f"RUN_BEFORE: request_id - {before_job_id} result: {result}")

                await self.update_job_status(
                    job_id, 'running', self.rpc_server.server_id
                )
                # start job
                name, args, kwargs = job['name'], job['args'], job['kwargs']

                results = None
                for _ in range(2):
                    results = await self.run_task(queue, name, args, kwargs)

                    if not results or results == f'task {name} failed':
                        if job['retry_policy'] == 'never':
                            break
                        continue
                    break

                if not results or results == f'task {name} failed':
                    self.log.warning(f'task {name} failed')
                    results = f'task {name} failed'
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

                if job['run_after']['run_after'] and not results == f'task {name} failed':
                    for job_name in job['run_after']['run_after']:
                        await self.run_job(queue, job_name, kwargs=results)
                
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
        self.log.debug(f"add_job: {job}")
        job_id = str(uuid.uuid1()) if not 'job_id' in job else job['job_id']

        new_job = {
            'status': 'queued'
        }
        new_job.update(job)

        for list_item in {'run_before', 'run_after'}:
            if not job[list_item]:
                job[list_item] = {list_item: []}
            else:
                job[list_item] = {list_item: job[list_item]}

        await self.db.tables['jobs'].insert(
            status='queued',
            **job
        )
        namespace = new_job['namespace']

        if not namespace in self.job_queues:
            await self.add_job_queue(namespace)
        
        # TODO - remove later moved this into job_proccessor
        #await self.job_queues[namespace].put(new_job)

        
        return job_id
    
    async def job_monitor(self):
        """
        monitors jobs in db.tables['jobs'] table


        """

    async def job_queue_distributor(self, job_queue):
        """
        Generator

        provides jobs from db.tables['jobs'] in a queued state
            without an owner
        """

        while True:
            try:
                #self.log.warning(f"job_queue_distributor: checking")
                queued = await self.db.tables['jobs'].select(
                    '*', 
                    where={
                        'node_id': None, 
                        'status': 'queued',
                        'namespace': job_queue
                    }
                )
                #breakpoint()
                if len(queued) == 0:
                    status = yield 'empty'
                    if status == 'finished':
                        raise asyncio.CancelledError(f"job_distributor finished")
                    continue
                for job in queued:
                    status = yield job
                    if status == 'finished':
                        raise asyncio.CancelledError(f"job_distributor finished")

            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                #self.log.exception(f"job_distributor error")
        self.log.warning(f"job_queue_distributor: exiting")


    async def add_job_results(self, job_id: str, results: dict):
        add_results = await self.db.tables['results'].insert(
            job_id=job_id,
            results=results
        )
        #await self.job_results[job_id].put(results)
    async def get_job_result_by_request_id(self, request_id):
        start = time.time()

        while time.time() - start < 60.0:
            queued_job = await self.db.tables['job_queue'][request_id]
            if queued_job is None:
                return f"No queued job with request_id {request_id} found" 
            if queued_job['job_id']:
                result = await self.get_job_result(queued_job['job_id'])
                self.log.debug(f"get_job_result_by_request_id completed in {time.time() - start:2f} s")
            await asyncio.sleep(1)
            continue
        return f"timeout waiting for request {request_id} in queue, job not created yet"

    async def get_job_result(self, job_id):
        start = time.time()
        while time.time() - start < 5.0:
            if not job_id in self.job_results:
                await asyncio.sleep(1)
                continue
            break

        if not job_id in self.job_results:
            raise Exception(f"no results found with job_id - {job_id}")

        result = await self.job_results[job_id].get()
        await self.db.tables['results'].delete(where={'job_id': job_id})
        await self.db.tables['job_queue'].delete(where={'job_id': job_id})
        del self.job_results[job_id]
        return result

    async def create_job_result_callback(self, job_id, worker_id):
        """
        accepts request_id returned by triggering a task
        """
        get_job_result = self.get_job_result(job_id)
        await self.create_callback(get_job_result, job_id, worker_id)
        return f"job_result callback created"

    async def create_task_subprocess_callback(self, request_id, worker_id):
        """
        invoked by workers to prepare manager for a subprocess task will be reporting
        results
        """
        get_subprocess_result = self.callback(request_id)
        await self.create_callback(get_subprocess_result, request_id, worker_id)
        return f"task_subprocess_callback created for request_id: {request_id} - worker_id: {worker_id}"

    async def create_callback(self, coro: Coroutine, request_id: str, worker_id: str):
        self.callback_results[request_id] = asyncio.Queue()
        async def call_back():
            try:
                results = await coro
                await self.rpc_server['manager'][f'add_callback_results_{worker_id}'](request_id, results)
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    self.log.exception(f"error with callback for request_id: {request_id}")

        if not worker_id in self.worker_callbacks:
            self.worker_callbacks[worker_id] = []
        self.worker_callbacks[worker_id].append(asyncio.create_task(call_back())) 
        return f"callback created - request_id: {request_id} - worker_id: {worker_id}"

    async def add_callback_results(self, request_id, results):
        """
        used to add results to a pending callback
        """
        await self.callback_results[request_id].put(results)
        return f"added callback result for request_id {request_id}"

    async def callback(self, request_id):
        """
        awaits results to be put to queue matching request_id via add_callback_results()
        then cleans up queue & returns results
        """
        try:
            result = await self.callback_results[request_id].get()
            del self.callback_results[request_id]
            return result
        except Exception as e:
            if not isinstance(e, asyncio.CancelledError):
                self.log.exception(f"error with callback for request_id: {request_id}")

