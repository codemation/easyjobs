import asyncio
import uuid, os, json
import logging
import random
import subprocess as sp
from easyrpc.server import EasyRpcServer
from easyrpc.register import Coroutine
from fastapi import FastAPI

server = FastAPI()

class EasyJobsWorker:
    def __init__(
        self,
        rpc_server: EasyRpcServer,
        rpc_proxy,
        max_tasks_per_worker: int = 3
    ):
        self.rpc_server = rpc_server
        self.rpc_proxy = rpc_proxy

        self.log = self.rpc_server.log
        self.workers = []
        self.MAX_TASKS_PER_WORKER = max_tasks_per_worker

        # queue of jobs to be sent to job_manager
        self.new_job_queue = asyncio.Queue()

        self.task_subprocess_results = {}
        
        self.task_callback_results = {}


    @classmethod
    async def create(
        cls,
        server: FastAPI,
        jobs_path: str,
        server_secret: str,
        manager_host: str,
        manager_port: str,
        manager_path: str,
        manager_secret: str,
        jobs_queue: str,
        max_tasks_per_worker: int = 3,
        logger = None,
        debug = False,
    ):
        rpc_server = await EasyRpcServer.create(
            server,
            jobs_path,
            server_secret,
            logger=logger,
            debug=debug
        )

        rpc_proxy = await rpc_server.create_server_proxy(
            manager_host,
            manager_port,
            manager_path,
            server_secret=manager_secret,
            namespace=f'{jobs_queue}'
        )

        await rpc_server.create_server_proxy(
            manager_host,
            manager_port,
            manager_path,
            server_secret=manager_secret,
            namespace=f'manager'
        )
        await asyncio.sleep(2)

        worker_id = '_'.join(rpc_proxy.session_id.split('-'))

        await rpc_server['manager']['add_worker_to_pool'](rpc_proxy.session_id)

        
        async def add_task_subprocess_results(request_id, results):
            return await jobs_worker.add_task_subprocess_results(request_id, results)
        
        add_task_subprocess_results.__name__ = f'add_task_subprocess_results_{worker_id}'
        rpc_server.origin(add_task_subprocess_results, namespace='manager')


        jobs_worker = cls(
            rpc_server,
            rpc_proxy,
            max_tasks_per_worker,
        )
        await jobs_worker.start_queue_workers(jobs_queue)
        await asyncio.sleep(2)

        return jobs_worker

    def task(
        self,
        namespace: str = 'DEFAULT',
        on_failure: str =  None, # on failure job
        retry_policy: str =  'retry_once', # retry_once, retry_always, never
        run_after: str = None,
        subprocess: bool = False,
    ):

        worker_id = '_'.join(self.rpc_proxy.session_id.split('-'))

        def job_register(func):
            self.rpc_server.origin(func, namespace=f'local_{namespace}')
            func_name = f"{func.__name__}"
            self.log.warning(f"registering {func_name} in job_manager")

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
                self.log.debug(f"new_job: {new_job}")
                await self.new_job_queue.put(new_job)
                self.log.debug(f"added new job {new_job['name']} to job_sender queue")
                return job_id
            if subprocess:
                REQUIRED_ENV_VARS = {'WORKER_TASK_DIR'}
                for var in REQUIRED_ENV_VARS:
                    if os.environ.get(var) is None:
                        raise Exception(f"missing env var {var} - required to use ")

                WORKER_TASK_DIR = os.environ.get('WORKER_TASK_DIR')

                if not os.system(f"ls {WORKER_TASK_DIR}/{func_name}.py") == 0:
                    template_location = 'https://github.com/codemation/easyjobs/tree/main/easyjobs/workers'
                    raise Exception(
                        f"missing expected {WORKER_TASK_DIR}/{func_name}.py task_subprocess file, create using template located here: {template_location}"
                    )
                async def task(*args, **kwargs):
                    request_id = str(uuid.uuid1())
                    self.task_subprocess_results[request_id] = asyncio.Queue()
 
                    # start task_subprocess
                    arguments = json.dumps({'args': list(args), 'kwargs': kwargs})
                    p = sp.Popen(
                        [
                            'python', 
                            f'{WORKER_TASK_DIR}/{func_name}.py',
                            self.rpc_proxy.origin_host,
                            str(self.rpc_proxy.origin_port),
                            self.rpc_proxy.origin_path,
                            self.rpc_proxy.server_secret,
                            request_id,
                            arguments
                        ]
                    )
                    # create_task_subprocess_callback - use 
                    await self.create_task_subprocess_callback(request_id, worker_id)

                    try:
                        return await self.task_subprocess_callback(request_id)
                    except Exception as e:
                        if not isinstance(e, asyncio.CancelledError):
                            return f'task {func_name} failed'
            else:
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
            
            job.__name__ = f'job_{func_name}_job_{worker_id}'
            
            self.rpc_server.origin(job, namespace=namespace)
            self.rpc_server.origin(job, namespace=f'local_{namespace}')

            task.__name__ = f"task_{func_name}_task_{worker_id}"
            self.rpc_server.origin(task, namespace=namespace)
            self.rpc_server.origin(task, namespace=f'local_{namespace}')

            asyncio.create_task(self.add_task(namespace, func_name))

            return job
        return job_register
    async def add_task_callback_result(self, request_id, results):
        """
        when task is triggered, callback id is issued to requestor 
        this id can be used to monitor the associated task_callback_results
        queue for results
        """
        await self.task_callback_results[request_id].put(results)
        return f"{request_id} results added"
    

    async def create_task_subprocess_callback(self, request_id, worker_id):
        return await self.rpc_server['manager']['create_task_subprocess_callback'](request_id, worker_id)        

    async def add_task_subprocess_results(self, request_id, results):
        await self.task_subprocess_results[request_id].put(results)
        self.log.debug(f"added {request_id} results")
        return f"added {request_id} results"

    async def task_subprocess_callback(self, request_id):
        """
        waits on a task_subprocess to complete then sendsresults
        """
        try:
            return await self.task_subprocess_results[request_id].get()
        except Exception as e:
            if not isinstance(e, asyncio.CancelledError):
                self.log.exception(f"error with task_subprocess_callback for request_id: {request_id}")
                
    async def add_task(self, queue, task_name):
        return await self.rpc_server['manager']['add_task'](queue, task_name)

    async def add_job_queue(self, queue: str):
        return await self.rpc_server['manager']['add_job_queue'](queue)

    async def add_job_to_queue(self, queue: str, job: dict):
        return await self.rpc_server['manager']['add_job_to_queue'](queue, job)
    
    async def add_job_results(self, job_id: str, results: dict):
        return await self.rpc_server['manager']['add_job_results'](
            job_id, results
        )
    async def get_job_result(self, job_id):
        return await self.rpc_server['manager']['get_job_result'](job_id)

    async def get_job_from_queue(self, queue):
        return await self.rpc_server['manager']['get_job_from_queue'](queue)
    
    async def update_job_status(self, job_id: str, status: str, node_id: str = None):
        return await self.rpc_server['manager']['update_job_status'](
            job_id, status, node_id
        )
    async def requeue_job(self, queue, job):
        return await self.rpc_server['manager']['requeue_job'](queue, job)
    
    async def delete_job(self, job_id):
        return await self.rpc_server['manager']['delete_job'](job_id)

    async def start_queue_workers(self, queue: str):
        self.workers.append(
            asyncio.create_task(self.job_sender())
        )
        self.log.warning(f"start_queue_workers - queue {queue} - callled ")
        for _ in range(self.MAX_TASKS_PER_WORKER):
            self.workers.append(
                asyncio.create_task(
                    self.worker(queue)
                )
            )
    def get_local_worker_task(self, queue, task_name, task_type):
        worker_id = '_'.join(self.rpc_proxy.session_id.split('-'))
        local_funcs = self.rpc_server[f'local_{queue}']
        self.log.debug(f"get_local_worker_task - local_funcs: {local_funcs}")
        return local_funcs.get(f'{task_type}_{task_name}_{task_type}_{worker_id}')

    def get_random_worker_task(self, queue, task_name, task_type):
        """
        pick a random worker with given task_name & task_type in queue
        """
        global_funcs = self.rpc_server[f'{queue}']
        worker_funcs = []
        for func in global_funcs:
            if f'{task_type}_{task_name}_{task_type}' in func:
            #if task_name in func and task_type in func and :
                worker_funcs.append(global_funcs[func])
        if worker_funcs:
            return random.choice(worker_funcs)
        return worker_funcs
    async def run_job(self, queue, name, args, kwargs):
        return await self.rpc_server['manager']['run_job'](
            queue, name, args, kwargs
        )

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
        return results

    async def job_sender(self):
        self.log.warning(f"starting job_sender")
        while True:
            try:
                self.log.debug(f"job_sender queue status: {self.new_job_queue}")
                job = await self.new_job_queue.get()
                queue = job['namespace']
                await self.add_job_to_queue(queue, job)
                self.log.debug(f"job_sender - added job {job['name']} to job_manager queue")
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"error with job_sender")
        self.log.warning(f"job_sender exiting")

    async def worker(self, queue):
        self.log.warning(f"worker started - for queue {queue}")
        while True:
            try:
                job = await self.get_job_from_queue(queue)
                if not isinstance(job, dict):
                    if 'KeyError' in job and queue in job:
                        self.log.warning(f"worker queue empty - sleeping (2) sec")
                        await asyncio.sleep(2)
                        continue
                    raise Exception(job)
                if 'queue_empty' in job:
                    self.log.warning(f"worker queue empty - sleeping (2) sec")
                    await asyncio.sleep(2)
                    continue

                job_id = job['job_id']
                
                # mark running
                await self.update_job_status(
                    job_id, 'running', self.rpc_server.server_id
                )
                self.log.debug(f"worker running job: {job}")
                # start job
                name, args, kwargs = job['name'], job['args'], job['kwargs']

                results = None
                for _ in range(2):
                    results = await self.run_task(queue, name, args, kwargs)

                    if results == f'task {name} failed':
                        if job['retry_policy'] == 'never':
                            break
                        continue
                
                self.log.debug(f"worker - results: {results}")

                if results == f'task {name} failed':
                    if job['on_failure']: 
                        on_failure = self.get_local_worker_task(queue, job['on_failure'], 'job')
                        if not on_failure is None:
                            await on_failure(job_failed=results)
                        else:
                            await self.run_job(queue, job['on_failure'], kwargs={'job_failed': results})
                    if job['retry_policy'] == 'retry_always':
                        await self.update_job_status(
                            job_id, 'queued', None
                        )
                        await self.requeue_job(queue, job)
                        continue

                # update results
                await self.add_job_results(job_id, results)

                # delete job 
                await self.delete_job(job_id)

                if job['run_after']:
                    run_after = self.get_local_worker_task(queue, job['run_after'], 'job')
                    if not run_after is None:
                        await run_after(**results)
                    else:
                        await self.run_job(queue, job['run_after'], kwargs=results)              
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"error in worker")
                await asyncio.sleep(5)
        self.log.warning(f"worker exiting")