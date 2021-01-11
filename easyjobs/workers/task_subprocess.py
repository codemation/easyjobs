"""
template for creating subprocess work - for long running / blocking code
return type should be json serializable - i.e json.dumps() capable
"""
import time
from easyjobs.workers.task import subprocess

@subprocess
def work(*args, **kwargs):
    print(f"received work request with args: {args} - kwargs: {kwargs}")
    """
    insert work here
    """
    time.sleep(5) # Blocking
    return {'args': list(args), 'kwargs': kwargs, 'result': 'I slept for 5 seconds - blocking'}

if __name__ == '__main__':
    work()
