import time
from easyjobs.workers.task import subprocess

@subprocess
def work():
    print(f"received work request in advanced blocking")
    """
    insert work here
    """
    time.sleep(10) # Blocking
    return {'result': 'I slept for 10 seconds - blocking'}
    
if __name__ == '__main__':
    work()