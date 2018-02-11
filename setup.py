import os
from setuptools import setup

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(SCRIPT_DIR, 'README.rst'), 'r') as readme_file:
    readme = readme_file.read()

with open(os.path.join(SCRIPT_DIR, 'VERSION'), 'r') as f:
    version = f.read().strip()

setup(
    name='uriutils',
    version=version,
    description='Easily read and write to different storage platforms in Python.',
    long_description=readme,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Internet',
        'Topic :: System :: Filesystems',
    ],
    keywords='boto3 s3 google cloud filesystem file uri url http ftp',
    url='http://github.com/skylander86/uriutils',
    author='Yanchuan Sim',
    author_email='yanchuan@outlook.com',
    license='Apache License 2.0',
    packages=['uriutils'],
    zip_safe=True,
)
