import asyncio, sys, json, time
from easyrpc.proxy import EasyRpcProxy
from easyrpc.register import Coroutine

async def job(mgr_addr, mgr_port, mgr_path, mgr_secret, request_id, results):
    proxy = await EasyRpcProxy.create(
        mgr_addr,
        int(mgr_port), 
        mgr_path, 
        server_secret=mgr_secret,
        namespace='manager'
    )
    await asyncio.sleep(0.5)
    await proxy[f'add_task_subprocess_results'](request_id, results)
    
def subprocess(f):
    print(f"task subprocess called with {sys.argv}")
    def task():
        mgr_addr, mgr_port, mgr_path, mgr_secret, request_id, arguments = sys.argv[1:] 

        arguments = json.loads(arguments)

        args, kwargs = arguments['args'], arguments['kwargs']

        results = f(*args, **kwargs)
        if isinstance(results, Coroutine):
            results = asyncio.run(results)

        asyncio.run(
            job(
                mgr_addr,
                mgr_port, 
                mgr_path, 
                mgr_secret, 
                request_id, 
                results
            )
        )
    return task