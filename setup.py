from setuptools import setup

setup(
    name='uriutils',
    version='0.1.9',
    description='Easily read and write to different storage platforms in Python.',
    long_description=open('README.rst', 'r').read(),
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
