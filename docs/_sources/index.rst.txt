uriutils-|version| documentation
================================

Welcome to the documentation for *uriutils*.
This package aims to make it transparent to the user and the developer the underlying storage system (i.e., S3, Google Cloud, local filesystems, etc) by wrapping the different protocols in a common interface.

Currently, the following storage systems are supported:

* Local filesystem (i.e., empty or ``file`` scheme)
* Amazon Web Services Simple Storage Services (S3) using :mod:`boto3` (i.e., ``s3`` scheme)
* Amazon Web Services Simple Notification Service (SNS) using :mod:`boto3` (i.e., ``sns`` scheme)
* Google Cloud Storage using :mod:`google.cloud.storage.client` (i.e., ``gcs`` or ``gs`` scheme)
* HTTP using :mod:`requests` (i.e., ``http`` or ``https`` scheme)

.. toctree::
   :maxdepth: 2

   api
   storage

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
