# # Off-Sample Orchestrator

## Considerations
This repo has been tested on Python 3.11. Conda is recommended.
```bash
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh

~/miniconda3/bin/conda init bash
conda create -n python311 python=3.11
```

## Building and installing the package

```bash
python -m pip install --upgrade build
python -m build
pip install dist/off_sample_orchestrator-*.whl
```
Or install directly from the latest version in the repository:
```bash
pip install git+https://github.com/ZikBurns/off_sample_orchestrator
```

## Install PyTorch CPU version
```bash
pip install torch==2.1.0 torchvision==0.16.0 --index-url https://download.pytorch.org/whl/cpu
```

## Task Manager
This is the code that will be executed inside each function.
Is called Task Manager because it defines a set of tasks: download, preprocess and inference. 
The inputs (images) will go through the tasks sequentially. 
The user can specify the type (threading/processing) and amout of parallelism within each task.


## General mechanism
The orchestrator runs invoking TaskManagers to execute the inference on datasets. 
The dataset will create a job for the orchestrator.
The job will be divided in splits and each split will be sent to a TaskManager to be processed.
The split size and number of task managers can be configured in the Job.

## Initialization
The orchestrator currently supports local and AWS Lambda execution backends.

```bash
from off_sample_orchestrator import Orchestrator

orchestrator = Orchestrator(fexec_args={'runtime': 'off_sample_311', 'runtime_memory': 3008}, initialize=False)
```
Has the following important parameters:
- fexec_args: Dictionary with the lithops FunctionExecutor arguments.
- orchestrator_backend: List of backends to be used by the orchestrator. Default is ['aws_lambda', 'local'].
- initialize: If True, the orchestrator will initialize aws_lambda function. If the backend is not deployed, it will do so.
- job_policy: Policy to be used by the orchestrator to execute the job. Default is 'default'.
- ec2_host_machine: If True, the orchestrator will deploy the function in the same VPC, subnet and security group as the orchestrator.
- max_job_managers: Maximum number of simultaneous jobs.
- max_task_managers: Maximum number of simultaneous functions.

## Task Manager deployment
Local backend doesn't need any deployment.
Deployment of task managers in AWS Lambda can be done using the initialize parameter in the Orchestrator or using deploy_function.
```bash
from off_sample_orchestrator import Orchestrator

orchestrator = Orchestrator(fexec_args={'runtime': 'off_sample_311', 'runtime_memory': 3008}, initialize=False)
orchestrator.redeploy_runtime()
```

Example of creation of a job.
```bash
job = Job(urls, job_name, orchestrator_backend="aws_lambda", split_size=split_size, num_task_managers=num_task_managers)
```
These are the most important parameters:
- input: List of URLs to be processed.
- job_name: Name of the job.
- bucket: Bucket from which the files will be downloaded. If not specified, the files will be downloaded from the internet.
- split_size: Size of the split to be processed by the task manager.
- num_task_managers: Number of task managers to be used.
- orchestrator_backend: Backend to be used by the orchestrator. Default is 'aws_lambda'.
- speculation_enabled: If True, the orchestrator will take care of stuck splits and reassign them to other task managers.
- keep_alive: If True, the task manager will ping the orchestrator to keep the connection alive.
- output_storage: Storage to be used to store the results ('local' or 's3'). By default, the results are printed, not stored.
- output_location: Location where the results will be stored. Can be disk location or location in object storage.
- output_bucket: Bucket where the results will be stored.


Even though the user can specify a split_size and a num_task_managers, the orchestrator will apply the following rules in order:
1. Default values. If the user does not specify a split_size or a num_task_managers, the orchestrator will use default values. Find them in [constants.py](off_sample_orchestrator%2Fconstants.py).
2. Job_policy. A job policy can be declared to specify how the job will be executed. The default job policy has 1 split per task manager. Other job policies may use more splits than task managers. If that's the case, task managers will ask the orchestrator for more splits if needed. Find more in [job_policies.py](off_sample_orchestrator%2Fjob_policies.py)
3. Maximum values for number of task managers. If the policy applied requires more task managers than the maximum allowed, the job will use less task managers.


## Job submission

To execute a job, it needs to be submitted to the orchestrator.
There are two ways:
1. Submit the job and wait for it to finish.
```bash
result=orchestrator.run_job(job)
```

2. Enqueue the job to the job queue and expect the results to be stored in disk or uploaded to cloud storage. This option supports executing multiple jobs at the same time.
```bash
orchestrator.enqueue_job(job)
```
To specify the directory where the results will be stored or the cloud storage bucket, configure the Job to do so. 

The orchestrator will control the maximum amount of simultaneous jobs and tasks, without surpassing the maximum amount of resources available. 


