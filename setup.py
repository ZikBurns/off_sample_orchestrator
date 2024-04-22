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
        'lithopserve @ git+https://github.com/ZikBurns/lithopserve'
    ],
    dependency_links = [
        'https://download.pytorch.org/whl/cpu/torch-2.0.1%2Bcpu-cp311-cp311-linux_x86_64.whl',
        'https://download.pytorch.org/whl/cpu/torchvision-0.15.2%2Bcpu-cp311-cp311-linux_x86_64.whl'
    ]
)

