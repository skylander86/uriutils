uriutils 0.1
============

.. image:: https://img.shields.io/pypi/v/uriutils.svg
    :target: https://pypi.python.org/pypi/uriutils

.. image:: https://readthedocs.org/projects/uriutils/badge/?version=latest
    :target: http://uriutils.readthedocs.io/en/latest/?badge=latest

Working with multiple storage platforms (i.e., local filesystem, S3, Google Cloud, etc.) can be quite a hassle.
This package aims to make it transparent to the user and the developer the underlying storage system by wrapping the different protocols in a common interface.

Documentation available at [http://uriutils.readthedocs.io/](http://uriutils.readthedocs.io/).

Usage
-----

Example::

    with uri_open('http://www.example.com', mode='r') as f:
        contents = f.read()

Example with argument parser::

    parser = ArgumentParser(description='Read text file from URI.')
    parser.add_argument('-i', '--input', type=URIFileType('r'), metavar='<input>', help='Input file URI.')
    A = parser.parse_args()

    contents = A.input.read()
    print(contents)

Or, writing to a file with argument parser is as easy as::

    parser = ArgumentParser(description='Write text file to URI.')
    parser.add_argument('-o', '--output', type=URIFileType('w'), metavar='<output>', help='Output file URI.')
    A = parser.parse_args()

    A.output.write('Hello world!\n')
    A.output.close()


And you can run ``python uri.py --output s3://example-bucket/output.txt``.

For complete documentation, please see `uriutils-0.1 documentation <http://uriutils.readthedocs.io>`_.

Contribution
------------

For bugs and issues, please file them on the `issues <https://github.com/skylander86/uriutils/issues>`_ page.
Thanks!
