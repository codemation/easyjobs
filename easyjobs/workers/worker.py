import asyncio
import uuid, os, json
import logging
import random
import subprocess as sp
from typing import Callable, Optional
from easyrpc.server import EasyRpcServer
from easyrpc.register import Coroutine, get_signature_as_dict
from fastapi import FastAPI

server = FastAPI()

class EasyJobsWorker:
    def __init__(
        self,
        rpc_server: EasyRpcServer,
        rpc_proxy,
        jobs_queue: str = 'DEFAULT',
        max_tasks_per_worker: int = 3
    ):
        self.rpc_server = rpc_server
        self.rpc_proxy = rpc_proxy
        self.jobs_queue = jobs_queue

        self.log = self.rpc_server.log
        self.workers = []
        self.pause_workers = False
        self.MAX_TASKS_PER_WORKER = max_tasks_per_worker

        # queue of jobs to be sent to job_manager
        self.new_job_queue = asyncio.Queue()

        # list of tasks which should be created via add_task
        self.local_tasks = []

        self.callbacks_pending = 0
        self.callback_status = asyncio.Queue()
        self.callback_results = {}
                    

        async def task_monitor():
            """
            job that monitors local_tasks in namespaces
            tasks in self.local_tasks are tasks registered by workers at startup
            if the local task does not exist in the namespace, the manager might be down or
            restarting
            """
            #restart_workers = False
            try:
                for task in self.local_tasks:
                    namespace = task['namespace']
                    name = task['name']
                    sig_config = task['sig']
                    schedule = task['schedule']
                    default_args = task['default_args']
                    if not name in self.rpc_server[namespace]:
                        #restart_workers = True
                        self.log.warning(f"task {name} not found in {namespace} - calling add_task on manager")
                        result = await self.add_task(namespace, name, sig_config, schedule, default_args)
                        self.log.warning(f"add_task result: {result}")
            except Exception as e:
                self.log.error(f"task_monitor - error checking local_tasks, maybe the JobsManager is down / restarting")
                for worker in self.workers:
                    worker.cancel()
                self.workers = []
                await self.start_queue_workers(self.jobs_queue)

        
        # run task_monitor at 30 sec intervals
        self.rpc_proxy.run_cron(task_monitor, 30)

    @classmethod
    async def create(
        cls,
        server: FastAPI,
        server_secret: str,
        manager_host: str,
        manager_port: str,
        manager_secret: str,
        jobs_queue: str,
        max_tasks_per_worker: int = 3,
        logger = None,
        debug = False,
    ):
        rpc_server = await EasyRpcServer.create(
            server,
            '/ws/jobs',
            server_secret,
            logger=logger,
            debug=debug
        )

        rpc_proxy = await rpc_server.create_server_proxy(
            manager_host,
            manager_port,
            '/ws/jobs',
            server_secret=manager_secret,
            namespace=f'{jobs_queue}'
        )

        await rpc_server.create_server_proxy(
            manager_host,
            manager_port,
            '/ws/jobs',
            server_secret=manager_secret,
            namespace=f'manager'
        )
        await asyncio.sleep(2)

        worker_id = '_'.join(rpc_proxy.session_id.split('-'))

        await rpc_server['manager']['add_worker_to_pool'](rpc_proxy.session_id)

        async def add_callback_results(request_id, results):
            return await jobs_worker.add_callback_results(request_id, results)

        add_callback_results.__name__ = f'add_callback_results_{worker_id}'
        rpc_server.origin(add_callback_results, namespace='manager')


        jobs_worker = cls(
            rpc_server,
            rpc_proxy,
            jobs_queue,
            max_tasks_per_worker
        )
        await jobs_worker.start_queue_workers(jobs_queue)
        await asyncio.sleep(2)

        return jobs_worker

    def task(
        self,
        on_failure: Optional[str] =  None, # on failure job
        retry_policy: Optional[str] =  'retry_once', # retry_once, retry_always, never
        run_before: Optional[list] = None,
        run_after: Optional[list] = None,
        subprocess: Optional[bool] = False,
        schedule: Optional[str] = None,
        default_args: Optional[dict] = None
    ):

        namespace = self.jobs_queue
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
                    'run_before': run_before,
                    'run_after': run_after
                }
                await self.new_job_queue.put(new_job)
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
                    self.callback_results[request_id] = asyncio.Queue()
 
                    # start task_subprocess
                    arguments = json.dumps({'args': list(args), 'kwargs': kwargs})
                    p = sp.Popen(
                        [
                            'python', 
                            f'{WORKER_TASK_DIR}/{func_name}.py',
                            self.rpc_proxy.origin_host,
                            str(self.rpc_proxy.origin_port),
                            self.rpc_proxy.server_secret,
                            request_id,
                            arguments
                        ]
                    )
                    # create_task_subprocess_callback - use 
                    await self.create_task_subprocess_callback(request_id, worker_id)

                    try:
                        return await self.callback(request_id)
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

            sig_config = {'name': func_name, 'sig': get_signature_as_dict(func)}
            job_config = {
                    'name': func_name, 
                    'namespace': namespace, 
                    'sig': sig_config, 
                    'schedule': schedule,
                    'default_args': default_args
                }
            self.local_tasks.append(job_config)

            async def run_job_and_get_result(*args, **kwargs):
                job_id = await job(*args, **kwargs)
                self.log.debug(f"run_job_and_get_result: job_id - {job_id}")
                cb_create = await self.create_job_result_callback(job_id)
                return await self.callback(job_id)
                

            return run_job_and_get_result
        return job_register
        
    async def callback_monitor(self):
        """
        task which monitors pending callbacks & expands workers if needed
        """

        self.callbacks_pending = 0
        self.callback_status = asyncio.Queue()
        self.callback_results = {}

        while True:
            try:
                status = await self.callback_status.get()
                self.log.debug(f"CALLBACK_MONITOR: status {status} - {self.callbacks_pending} / {self.MAX_TASKS_PER_WORKER} ## {self.callback_results.keys()}")
                if status == 'waiting':
                    self.callbacks_pending+=1
                if status == 'finished':
                    self.callbacks_pending-=1
                if self.callbacks_pending > self.MAX_TASKS_PER_WORKER:
                    # add worker
                    self.log.warning(f"CALLBACK_MONITOR: detected all workers - waiting, adding worker")
                    self.MAX_TASKS_PER_WORKER+=1
                    self.workers.append(
                        asyncio.create_task(
                            self.worker(self.jobs_queue)
                        )
                    )
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"callback_monitor error")
    async def create_task_subprocess_callback(self, request_id, worker_id):
        return await self.rpc_server['manager']['create_task_subprocess_callback'](request_id, worker_id)
    
    async def create_callback(self, request_id):
        worker_id = '_'.join(self.rpc_proxy.session_id.split('-'))
        return await self.rpc_server['manager']['create_callback'](request_id, worker_id)

    async def add_callback_results(self, request_id, results):
        await self.callback_results[request_id].put(results)
        return f"added callback result for request_id {request_id}"

    async def callback(self, request_id):
        """
        awaits results to be put to queue matching request_id via add_callback_results()
        then cleans up queue & returns results
        """
        try:
            await self.callback_status.put('waiting')
            result = await self.callback_results[request_id].get()
            del self.callback_results[request_id]
            await self.callback_status.put('finished')
            return result
        except Exception as e:
            if not isinstance(e, asyncio.CancelledError):
                self.log.exception(f"error with callback for request_id: {request_id}")

    async def create_job_result_callback(self, job_id):
        self.callback_results[job_id] = asyncio.Queue()
        worker_id = '_'.join(self.rpc_proxy.session_id.split('-'))
        return await self.rpc_server['manager']['create_job_result_callback'](job_id, worker_id)
                
    async def add_task(self, queue, task_name, sig_config, schedule, default_args):
        return await self.rpc_server['manager']['add_task'](queue, task_name, sig_config, schedule, default_args)

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

    """
    async def get_job_from_queue(self, queue):
        return await self.rpc_server['manager']['get_job_from_queue'](queue)

    """
    async def get_job_from_queue(self, queue, worker_id):
        request_id = str(uuid.uuid1())
        self.callback_results[request_id] = asyncio.Queue()
        await self.rpc_server['manager']['create_get_job_from_queue_callback'](
            queue, request_id, worker_id
        )
        job = await self.callback(request_id)
        self.log.warning(f"worker {worker_id} - pulled job {job} ")
        return job
    
    
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
        self.workers.append(
            asyncio.create_task(self.worker_monitor())
        )
        #self.workers.append(
        #    asyncio.create_task(self.callback_monitor())
        #)
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
                job = await self.new_job_queue.get()
                queue = job['namespace']
                job_add_result = await self.add_job_to_queue(queue, job)
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"error with job_sender")
        self.log.warning(f"job_sender exiting")

    async def worker_monitor(self):

        self.worker_status = asyncio.Queue()
        self.workers_working = 0
        self.log.warning(f"WORKER_MONITOR: starting")
        while True:
            try:
                status = await self.worker_status.get()
                
                if status == 'working':
                    self.workers_working+=1
                if status == 'finished':
                    self.workers_working-=1
                self.log.warning(f"WORKER_MONITOR: working {self.workers_working} / {self.MAX_TASKS_PER_WORKER}")
                if self.workers_working > self.MAX_TASKS_PER_WORKER:
                    self.log.warning(f"WORKER_MONITOR: detected max workers working, temporarily scaling by 1")
                    self.workers.append(
                        asyncio.create_task(
                            self.worker(self.jobs_queue)
                        )
                    )
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"WORKER_MONITOR: error")
        self.log.warning(f"WORKER_MONITOR: exiting")

    def toggle_workers(self, pause: bool):
        """
        Enables or disables workers ability to pull new jobs
        ::pause = False 
        """
        self.log.warning(f"toggle_workers triggered - pause set to {pause}")
        self.pause_workers = pause


    async def worker(self, queue):
        self.log.warning(f"worker started - for queue {queue}")
        worker_id = '_'.join(self.rpc_proxy.session_id.split('-'))
        while True:
            try:
                if self.pause_workers:
                    await asyncio.sleep(5)
                    continue
                job = await self.get_job_from_queue(queue, worker_id)
                await self.worker_status.put('working')
                if not isinstance(job, dict):
                    if 'KeyError' in job and queue in job:
                        self.log.debug(f"worker queue empty - sleeping (2) sec")
                        await asyncio.sleep(2)
                        continue
                    raise Exception(job)
                if 'queue_empty' in job:
                    self.log.debug(f"worker queue empty - sleeping (2) sec")
                    await asyncio.sleep(2)
                    continue

                job_id = job['job_id']

                self.log.debug(f"worker pulled {job} from queue")
                
                if job['run_before']['run_before']:
                    tasks = []
                    for task in job['run_before']['run_before']:
                        run_after = self.get_local_worker_task(queue, task, 'job')
                        if not run_after is None:
                            before_job_id = await run_after()
                        else:
                            before_job_id = await self.run_job(queue, task)
                        await self.create_job_result_callback(before_job_id)
                        tasks.append(before_job_id)

                    await self.update_job_status(
                        job_id, 'waiting', self.rpc_server.server_id
                    )
                    for before_job_id in tasks:
                        await self.callback(before_job_id)
                    
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
                    # no retries needed
                    break
                
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

                if job['run_after']['run_after'] and not results == f'task {name} failed':
                    for job_name in job['run_after']['run_after']:
                        run_after = self.get_local_worker_task(queue, job_name, 'job')
                        if not run_after is None:
                            await run_after(**results)
                        else:
                            await self.run_job(queue, job_name, kwargs=results)
                await self.worker_status.put('finished')
                if self.workers_working > self.MAX_TASKS_PER_WORKER:
                    self.log.warning(f"worker: exiting - max workers {self.MAX_TASKS_PER_WORKER} working / exceeded ")
                    break
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"error in worker")
                await asyncio.sleep(5)
        self.log.warning(f"worker exiting")

    async def add_task_results(self, request_id, results):
        await self.task_results[request_id].put(results)
        self.log.debug(f"added {request_id} results")
        return f"added {request_id} results"