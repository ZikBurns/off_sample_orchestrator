# Building the package
Python 3.11 is required. Conda is recommended:
```bash
python -m pip install --upgrade build
python -m build
pip install dist/off_sample_orchestrator-*.whl
```

# CloudSkin-EMBL-Orchestrator

## Installation
Python 3.11 is required. Conda is recommended:
```bash
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh

~/miniconda3/bin/conda init bash
conda create -n python311 python=3.11
```

Install dependencies:

```bash
pip install git+https://github.com/ZikBurns/lithops-inference
pip install -r requirements.txt
pip install torch==2.1.1 torchvision==0.16.1 torchaudio==2.1.1 --index-url https://download.pytorch.org/whl/cpu

```

## Function deployment
The orchestrator code is in [off_sample_orchestrator](off_sample_orchestrator) directory.
In included_function directory is the code that will be executed in the Lambda function or locally (for testing purposes).

To run the orchestrator in AWS, first there's a need to deploy the function to AWS.
To do that, run the script to deploy the function [deploy_function.py](lambda_test%2Fdeploy_function.py).

### EC2 deployment
In case the function is to be deployed on EC2, the orchestrator has an option to deploy the function in the same VPC, subnet and security group as the orchestrator.
To do that, toggle the ec2_host_machine=True in the orchestrator code.
```bash
from off_sample_orchestrator import Orchestrator

orchestrator = Orchestrator(fexec_args={'runtime': 'off_sample_311', 'runtime_memory': 3008}, ec2_host_machine=True, initialize=False)
```

## Usage
The orchestrator runs invoking TaskManagers (workers/lambdas) to execute the inference on datasets. 
The dataset will create a job for the orchestrator.
The job will be divided in splits and each split will be sent to a TaskManager to be processed.
The split size and number of task managers can be configured in the Job.
Example of creation of a job.
```bash
job = Job(urls, job_name, orchestrator_backend="aws_lambda", split_size=split_size, num_task_managers=num_task_managers)
```
Check Job description for more details.

Even though the user can specify a split_size and a num_task_managers, the orchestrator will apply:
1. Default values. If the user does not specify a split_size or a num_task_managers, the orchestrator will use default values. Find them in [constants.py](off_sample_orchestrator%2Fconstants.py).
2. Job_policy. A job policy can be declared to specify how the job will be executed. The default job policy has 1 split per task manager. Other job policies may use more splits than task managers. If that's the case, task managers will ask the orchestrator for more splits if needed. Find more in [job_policies.py](off_sample_orchestrator%2Fjob_policies.py)


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

The orchestrator will control the maximum amount of simultaneous jobs and tasks, without surpassing the maximum amount of resources available. These are declared in [constants.py](off_sample_orchestrator%2Fconstants.py).


