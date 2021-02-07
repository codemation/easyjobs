async def api_setup(manager):
    from typing import Optional
    from fastapi import HTTPException
    server = manager.rpc_server.server

    @server.get('/job/pull/result', tags=['Manager'])
    async def pull_job_result(
        job_id: Optional[str] = None, 
        request_id: Optional[str] = None
    ):
        try:
            if job_id:
                return await manager.get_job_result(job_id)
            if request_id:
                return await manager.get_job_result_by_request_id(request_id)
        except Exception as e:
            if 'no results found' in f'{repr(e)}':
                raise HTTPException(status_code=404, detail=f"no result found with provided id")

    @server.get('/job/view/result', tags=['Manager'])
    async def view_job_result(
        job_id: Optional[str] = None, 
        request_id: Optional[str] = None
    ):
        if job_id:
            return await manager.db.tables['results'].select(
                '*', where={'job_id': job_id}
            )
        if request_id:
            queued_job = await manager.db.tables['job_queue'][request_id]
            if not queued_job:
                return f"no job matching request_id {request_id}"
            result = await manager.db.tables['results'][queued_job['job_id']]
            if not result:
                return await manager.db.tables['jobs'][queued_job['job_id']]
            return result
    
    @server.get('/job/view/results', tags=['Manager'])
    async def view_job_results():
        return await manager.db.tables['results'].select('*')

    @server.get('/job/view/queued', tags=['Manager'])
    async def view_queued_jobs(
        namespace: Optional[str] = None
    ):
        job_filter = {}
        if namespace:
            job_filter['namespace'] = namespace
        return await manager.db.tables['job_queue'].select(
            '*', where=job_filter
        )

    @server.get('/job/view/running', tags=['Manager'])
    async def view_queued_jobs(
        namespace: Optional[str] = None, 
        name: Optional[str] = None,
        status: Optional[str] = None
    ):
        job_filter = {}
        if namespace:
            job_filter['namespace'] = namespace
        if name:
            job_filter['name'] = name
        if status:
            job_filter['status'] = status
        return await manager.db.tables['jobs'].select(
            '*', where=job_filter
        )