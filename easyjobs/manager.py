import asyncio
import uuid
import logging

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
                ('status', str),
                ('name', str),
                ('args', str),
                ('kwargs', str)
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
        print(f"closing db {db}")
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

        self.broker_type = broker_type
        self.broker_path = broker_path
        
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
        
        return job_manager

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
                self.message_consumer(generator)
            )
        )

    async def message_consumer(self, message_generator):
        self.log.warning(f"message_consumer - starting - for type {self.broker_type} ")
        
        while True:
            try:
                job = await message_generator.asend(None)
                print(f"message_consumer - job: {job}")
            except Exception as e:
                if isinstance(e, asyncio.CancelledError):
                    break
                self.log.exception(f"message_consumer exception")
        await message_generator.asend('finished')
        

    def task(self, func, namespace: str = 'DEFAULT'):
        self.rpc_server.origin(func, namespace=f'jobs_{namespace}')

        async def job(*args, **kwargs):
            new_job = {
                'namespace': namespace,
                'name': func.__name__,
                'args': {'args': list(args)},
                'kwargs': kwargs,
            }
            print(f"new_job: {new_job}")
            job_id = await self.add_job(new_job)
            return job_id
        job.__name__ = func.__name__

        self.rpc_server.origin(job, namespace=namespace)   
        return job

    async def add_job_queue(self, queue: str):
        if not queue in self.job_queues:
            self.job_queues[queue] = asyncio.Queue()
            await self.start_queue_workers(queue)
            await self.start_message_consumer(queue)
    
    async def load_job_queues(self):

        jobs = await self.db.tables['jobs'].select('*')
        for job in jobs:
            queue = job['namespace']
            if not queue in self.job_queues:
                await self.add_job_queue(queue)
            await self.job_queues[queue].put(job)

    async def start_queue_workers(self, queue: str, count: int = 1):
        for _ in range(count):
            self.workers.append(
                asyncio.create_task(
                    self.worker(queue)
                )
            )

    async def worker(self, queue):
        self.log.warning(f"worker started")
        while True:
            try:
                job = await self.job_queues[queue].get()
                job_id = job['job_id']
                
                # mark running
                await self.db.tables['jobs'].update(
                    status='running',
                    where={'job_id': job_id}
                )
                print(job)
                # start job
                name, args, kwargs = job['name'], job['args'], job['kwargs']
                print(type(args))

                results = self.rpc_server[f'jobs_{queue}'][name](*args['args'], **kwargs)
                if isinstance(results, Coroutine):
                    results = await results

                # update results
                await self.add_job_results(job_id, results)

                # delete job 
                await self.db.tables['jobs'].delete(
                    where={'job_id': job_id}
                )

                
            except Exception as e:
                self.log.exception(f"error in worker")
                # TODO-  asyncio.CancelledError 
                break
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
        job_id = str(uuid.uuid1())

        new_job = {
            'job_id': job_id,
            'status': 'queued'
        }
        new_job.update(job)


        await self.db.tables['jobs'].insert(
            job_id=job_id,
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
        result = await self.job_results[job_id].get()
        await self.db.tables['results'].delete(where={'job_id': job_id})
        return result