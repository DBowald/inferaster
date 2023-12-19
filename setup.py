from setuptools import setup, find_packages

setup( 
    name='inferaster',
    version='0.1.0',
    packages=find_packages(include=['inferaster', 'inferaster.*']),
    description='Util for downloading and chipping aerial/remote image data for paired image to image translation.',
    author='Dylan Bowald',
    author_email='dylanbowald@gmail.com'
    )