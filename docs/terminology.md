## Terminology

### EasyJobsManager
- responsible for pulling jobs from a broker 
- adds jobs to persistent database & global queue
- provides workers access to global queue for pulling jobs
- provides workers ability to store results to persistent database which can be pulled or pushed to a specificed message queue. 
- can act as a worker if task is defined locally within namespace
- Should NOT be forked
!!! Tip
    Work performed on a Manager should be as non-blocking as possible, since the main thread cannot be forked, long running / blocking code on a Manager will have adverse affects. When in doubt, put it on a separate worker.

### EasyJobsWorker
- Connects to a running EasyJobsManager and pulls jobs to run within a specified queue
- Runs Jobs and pushes results back to EasyJobsManager
- Process can be forked