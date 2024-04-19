from setuptools import setup, find_packages

setup(
    name='off_sample_orchestrator',
    version='0.1',
    description='An orchestrator of functions for image classification.',
    author='Josep Calero',
    author_email='josep.calero@urv.cat',
    packages = find_packages(),
    install_requires=[
        'urllib3',
        'flask',
        'requests',
        'setuptools',
        'boto3',
        'botocore',
        'pillow',
        'numpy',
        'pyyaml',
        'grpcio',
        'protobuf',
        'lithops-inference @ git+https://github.com/ZikBurns/lithops-inference'
    ],
    dependency_links = [
        'https://download.pytorch.org/whl/cpu/torch-2.1.1%2Bcpu-cp36-cp36m-linux_x86_64.whl',
        'https://download.pytorch.org/whl/cpu/torchvision-0.16.1-cp36-cp36m-linux_x86_64.whl'
    ]
)

