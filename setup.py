from setuptools import setup, find_packages

setup(
    name='off_sample_orchestrator',
    version='0.1',
    description='An orchestrator of functions for image classification.',
    author='Josep Calero',
    author_email='josep.calero@urv.cat',
    packages = find_packages(),
    install_requires=[
        'urllib3==1.26.18',
        'grpcio==1.58.0',
        'grpcio-tools==1.58.0',
        'flask==3.0.2',
        'requests==2.31.0',
        'setuptools==68.2.2',
        'boto3==1.34.55',
        'botocore==1.34.55',
        'pillow==10.2.0',
        'numpy==1.26.4',
        'pyyaml==6.0.1',
        'protobuf==4.24.0',
        'lithops-inference @ git+https://github.com/ZikBurns/lithops-inference'
    ],
    dependency_links = [
        'https://download.pytorch.org/whl/cpu/torch-2.1.1%2Bcpu-cp36-cp36m-linux_x86_64.whl',
        'https://download.pytorch.org/whl/cpu/torchvision-0.16.1-cp36-cp36m-linux_x86_64.whl'
    ]
)

