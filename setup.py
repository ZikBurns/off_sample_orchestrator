from setuptools import setup, find_packages

setup(
    name='off_sample_orchestrator',
    version='0.1',
    description='An orchestrator of functions for image classification.',
    author='Josep Calero',
    author_email='josep.calero@urv.cat',
    packages = find_packages(),
    install_requires=[
        'flask',
        'pillow',
        'numpy',
        'pyyaml',
        'grpcio==1.51.1',
        'protobuf==4.21.12',
        'lithops_serve @ git+https://github.com/ZikBurns/lithops-serve',
        'torch==2.0.1+cpu',
        'torchvision==0.15.2+cpu'

    ],
    dependency_links = ['https://download.pytorch.org/whl/cpu']
)

