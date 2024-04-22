from setuptools import setup, find_packages
import sys

# Get the python version
python_version = sys.version_info[:2]

wheel_urls = {
    (3, 11): {
        'torch': 'https://download.pytorch.org/whl/cpu/torch-2.0.1%2Bcpu-cp311-cp311-linux_x86_64.whl',
        'torchvision': 'https://download.pytorch.org/whl/cpu/torchvision-0.15.2%2Bcpu-cp311-cp311-linux_x86_64.whl',
    },
    (3, 10): {
        'torch': 'https://download.pytorch.org/whl/cpu/torch-2.0.1%2Bcpu-cp310-cp310-linux_x86_64.whl',
        'torchvision': 'https://download.pytorch.org/whl/cpu/torchvision-0.15.2%2Bcpu-cp310-cp310-linux_x86_64.whl',
    },
    # Add more versions if needed
}



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
        'lithops_serve @ git+https://github.com/ZikBurns/lithops-serve'
    ],
    dependency_links = [
        wheel_urls[python_version]['torch'],
        wheel_urls[python_version]['torchvision'],
    ],
)

