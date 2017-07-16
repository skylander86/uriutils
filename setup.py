from setuptools import setup

setup(
    name='uriutils',
    version='0.1',
    description='Easily read and writ to different storage platforms in Python.',
    long_description='Working with multiple storage platforms (i.e., local filesystem, S3, Google Cloud, etc.) can be quite a hassle. This package aims to make it transparent to the user and the developer the underlying storage system by wrapping the different protocols in a common interface.',
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
    license='Apache 2.0',
    packages=['uriutils'],
    zip_safe=True,
)
