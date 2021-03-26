##

### Installation

    $ virtualenv -p python3.7 easy-job-env

    $ source easy-jobs-env/bin/activate

    (easy-job-env)$ pip install easyjobs

### Supported Brokers - Pull Jobs
- rabbitmq
- easyjobs
- TODO - Amazon SQS

### Supported Producers 
- rabbitmq - Send jobs to rabbitmq first - consume later
- EasyJobs API - Create jobs directly via EasyJobsManager API