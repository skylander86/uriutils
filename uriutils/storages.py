"""
This module defines all the storage systems supported by uriutils.
"""

__all__ = ['STORAGES', 'URIBytesOutput', 'BaseURI']

from io import BytesIO
import os
import shutil
import warnings

try: from urlparse import urlparse  # Python 2
except ImportError: from urllib.parse import urlparse  # Python 3

try:
    import boto3
    import botocore.exceptions
except ImportError: boto3 = None

try:
    from google.cloud import storage as gcloud_storage
    import google.cloud.exceptions
except ImportError: gcloud_storage = None

try: import requests
except ImportError: requests = None


class URIBytesOutput(BytesIO):
    """A BytesIO object for output that flushes content to the remote URI on close."""

    def __init__(self, uri_obj):
        super(URIBytesOutput, self).__init__()
        self.uri_obj = uri_obj
    #end def

    def close(self):
        if not self.closed:
            self.uri_obj.put_content(self.getvalue())
            super(URIBytesOutput, self).close()
        #end if
    #end def

    @property
    def name(self):
        return str(self.uri_obj)
#end class


class BaseURI(object):
    """
    This is the base URI storage object that is inherited by the different storage systems.
    It defines the methods and operations that can be "conducted" on a URI.
    Almost all of these methods have to be implemented by a storage class.
    """

    SUPPORTED_SCHEMES = []
    """Defines the schemes supported by this storage system."""

    VALID_STORAGE_ARGS = []
    """The set of ``storage_args`` keyword arguments that is handled by this storage system."""

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        """
        Parses the URI and return an instantiation of the storage system if it is supported.

        :param str uri: URI to check
        :param dict storage_args: Keyword arguments to pass to the underlying storage object
        :returns: ``None`` if this storage system does not support :attr:`uri`.
        """

        raise NotImplementedError('`parse_uri` is not implemented for {}.'.format(type(cls).__name__))
    #end def

    def __init__(self, storage_args={}):
        """
        :param dict storage_args: Arguments that will be applied to the storage system for read/write operations
        """
        self.storage_args = {}
        for k in storage_args.keys():
            if k in self.VALID_STORAGE_ARGS:
                self.storage_args[k] = storage_args[k]
            else:
                warnings.warn('"{}" is not a valid storage argument.'.format(k), category=UserWarning, stacklevel=2)
        #end for
    #end def

    def get_content(self):
        """
        :returns: the bytestring stored at this object's URI
        :rtype: bytes
        """

        raise NotImplementedError('`get_content` is not implemented for {}.'.format(type(self).__name__))
    #end def

    def put_content(self, content):
        """
        :param bytes content: Content to write to this object's URI
        """

        raise NotImplementedError('`put_content` is not implemented for {}.'.format(type(self).__name__))
    #end def

    def download_file(self, filename):
        """
        Download the binary content stored in the URI for this object directly to local file.

        :param str filename: Filename on local filesystem
        """

        raise NotImplementedError('`download_file` is not implemented for {}.'.format(type(self).__name__))
    #end def

    def upload_file(self, filename):
        """
        Upload the binary content in ``filename`` to the URI for this object.

        :param str filename: Filename on local filesystem
        """

        raise NotImplementedError('`upload_file` is not implemented for {}.'.format(type(self).__name__))
    #end def

    def get_metadata(self):
        """
        :returns: the metadata associated with this object's URI
        :rtype: dict
        """

        return {}
    #end def

    def exists(self):
        """
        :returns: ``True`` if URI exists
        :rtype: bool
        """

        raise NotImplementedError('`exists` is not implemented for {}.'.format(type(self).__name__))
    #end def

    def dir_exists(self):
        """
        Check if the URI exists as a directory.

        :returns: ``True`` if URI exists as a directory
        :rtype: bool
        """

        raise NotImplementedError('`dir_exists` is not implemented for {}.'.format(type(self).__name__))
    #end def

    def make_dir(self):
        """Create a directory."""

        raise NotImplementedError('`make_dir` is not implemented for {}.'.format(type(self).__name__))

    def list_dir(self):
        """List the contents of a directory."""

        raise NotImplementedError('`list_dir` is not implemented for {}.'.format(type(self).__name__))

    def join(self, path):
        """
        Similar to :func:`os.path.join` but returns a storage object instead.

        :param str path: path to join on to this object's URI
        :returns: a storage object
        :rtype: BaseURI
        """

        return self.parse_uri(urlparse(os.path.join(str(self), path)), storage_args=self.storage_args)

    def __str__(self):
        """
        :returns: a nicely formed URI for this object.
        """

        raise NotImplementedError('`__str__` is not implemented for {}.'.format(type(self).__name__))
    #end def

    def __unicode__(self):
        return self.__str__()

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, str(self))
#end class


class FileURI(BaseURI):
    """
    Storage system for local filesystem.

    :param str filepath: Local file path
    :param dict storage_args: Keyword arguments that are passed to :func:`open`
    """

    SUPPORTED_SCHEMES = set(['file', ''])
    """Supported schemes for :class:`FileURI`."""

    VALID_STORAGE_ARGS = ['mode', 'buffering', 'encoding', 'errors', 'newline', 'closefd', 'opener']
    """Storage arguments allowed to pass to :meth:`open` methods."""

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        if uri.scheme not in cls.SUPPORTED_SCHEMES: return None
        filepath = os.path.join(uri.netloc, uri.path.lstrip('/')).rstrip('/') if uri.netloc else uri.path
        return FileURI(filepath, storage_args=storage_args)
    #end def

    def __init__(self, filepath, storage_args={}):
        super(FileURI, self).__init__(storage_args=storage_args)
        self.filepath = filepath
    #end def

    def get_content(self):
        with open(self.filepath, 'rb', **self.storage_args) as f:
            return f.read()
    #end def

    def put_content(self, content):
        with open(self.filepath, 'wb', **self.storage_args) as f:
            return f.write(content)

    def download_file(self, filename):
        shutil.copyfile(self.filepath, filename)
    #end def

    def upload_file(self, filename):
        shutil.copyfile(filename, self.filepath)

    def exists(self):
        return os.path.exists(self.filepath)

    def dir_exists(self):
        return os.path.isdir(self.filepath)

    def make_dir(self):
        os.makedirs(self.filepath)

    def list_dir(self):
        for fname in os.listdir(self.filepath):
            yield os.path.join(self.filepath, fname)

    def __str__(self):
        return self.filepath
#end class


class S3URI(BaseURI):
    """
    Storage system for AWS S3.
    """

    SUPPORTED_SCHEMES = set(['s3'])
    """Supported schemes for :class:`S3URI`."""

    VALID_STORAGE_ARGS = ['ACL', 'CacheControl', 'ContentDisposition', 'ContentEncoding', 'ContentLanguage', 'ContentLength', 'ContentMD5', 'ContentType', 'Expires', 'GrantFullControl', 'GrantRead', 'GrantReadACP', 'GrantWriteACP', 'Metadata', 'ServerSideEncryption', 'StorageClass', 'WebsiteRedirectLocation', 'SSECustomerAlgorithm', 'SSECustomerKey', 'SSEKMSKeyId', 'RequestPayer', 'Tagging']
    """Storage arguments allowed to pass to :class:`S3.Client` methods."""

    s3_resource = None

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        if uri.scheme not in cls.SUPPORTED_SCHEMES: return None
        if boto3 is None: raise ImportError('You need to install boto3 package to handle {} URIs.'.format(uri.scheme))

        if cls.s3_resource is None: cls.s3_resource = boto3.resource('s3')

        return S3URI(uri.netloc, uri.path.lstrip('/'), storage_args=storage_args)
    #end def

    def __init__(self, bucket, key, storage_args={}):
        """
        :param str bucket: Bucket name
        :param str key: Key to file
        :param dict storage_args: Keyword arguments that are passed to :class:`S3.Client`
        """

        super(S3URI, self).__init__(storage_args=storage_args)

        self.s3_object = self.s3_resource.Object(bucket, key)
    #end def

    def get_content(self):
        r = self.s3_object.get(**self.storage_args)
        return r['Body'].read()
    #end def

    def put_content(self, content):
        self.s3_object.put(Body=content, **self.storage_args)

    def download_file(self, filename):
        self.s3_object.download_file(filename, **self.storage_args)

    def upload_file(self, filename):
        self.s3_object.upload_file(filename, ExtraArgs=self.storage_args)

    def get_metadata(self):
        """Uses ``HEAD`` requests for efficiency."""

        self.s3_object.load()
        return self.s3_object.metadata

    def exists(self):
        """Uses ``HEAD`` requests for efficiency."""

        try:
            self.s3_object.load()
            return True
        except botocore.exceptions.ClientError: return False
    #end def

    def dir_exists(self): return True

    def make_dir(self):
        """Ignored for S3."""
        pass

    def list_dir(self):
        """
        Non-recursive file listing.

        :returns: A generator over files in this "directory" for efficiency.
        """

        bucket = self.s3_object.Bucket()
        prefix = self.s3_object.key
        if not prefix.endswith('/'): prefix += '/'

        for obj in bucket.objects.filter(Delimiter='/', Prefix=prefix):
            yield 's3://{}/{}'.format(obj.bucket_name, obj.key)
    #end def

    def __str__(self):
        return 's3://{}/{}'.format(self.s3_object.bucket_name, self.s3_object.key)
#end class


class GoogleCloudStorageURI(BaseURI):
    """
    Storage system for Google Cloud storage.
    """

    SUPPORTED_SCHEMES = set(['gs', 'gcs'])
    """Supported schemes for :class:`GoogleCloudStorageURI`."""

    VALID_STORAGE_ARGS = ['chunk_size', 'encryption_key']
    """Storage arguments allowed to pass to :mod:`google.cloud.storage.client` methods."""

    gs_client = None

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        if uri.scheme not in cls.SUPPORTED_SCHEMES: return None
        if gcloud_storage is None: raise ImportError('You need to install google-cloud-storage package to handle {} URIs.'.format(uri.scheme))

        if cls.gs_client is None: cls.gs_client = gcloud_storage.Client()

        return GoogleCloudStorageURI(uri.netloc, uri.path.lstrip('/'), storage_args=storage_args)
    #end def

    def __init__(self, bucket, key, storage_args={}):
        """
        :param str bucket: Bucket name
        :param str key: Key to file
        :param dict storage_args: Keyword arguments that are passed to :mod:`google.cloud.storage.client`
        """

        self.content_type = storage_args.get('content_type', 'application/octet-stream')
        self.content_encoding = storage_args.get('content_encoding', None)
        self.metadata = storage_args.get('metadata', {})
        self.metadata.update(storage_args.get('Metadata', {}))

        super(GoogleCloudStorageURI, self).__init__(storage_args=storage_args)

        self.blob = self.gs_client.bucket(bucket).blob(key, **self.storage_args)
    #end def

    def get_content(self):
        return self.blob.download_as_string()

    def put_content(self, content):
        """
        The default content type is set to ``application/octet-stream`` and content encoding set to ``None``.
        """

        self.blob.content_encoding = self.content_encoding
        self.blob.metadata = self.metadata
        return self.blob.upload_from_string(content, content_type=self.content_type)

    def download_file(self, filename):
        self.blob.download_to_filename(filename)

    def upload_file(self, filename):
        self.blob.content_encoding = self.content_encoding
        self.blob.metadata = self.metadata
        self.blob.upload_from_filename(filename, content_type=self.content_type)

    def get_metadata(self):
        """Uses ``HEAD`` requests for efficiency."""

        self.blob.reload()
        return self.blob.metadata

    def exists(self):
        """Uses ``HEAD`` requests for efficiency."""

        try:
            self.blob.reload()
            return True
        except google.cloud.exceptions.NotFound: return False
    #end def

    def dir_exists(self): return True

    def make_dir(self): pass

    def list_dir(self):
        """
        Non-recursive file listing.

        :returns: A generator over files in this "directory" for efficiency.
        """

        bucket = self.blob.bucket
        prefix = self.blob.name
        if not prefix.endswith('/'): prefix += '/'

        for blob in bucket.list_blobs(prefix=prefix, delimiter='/'):
            yield 'gs://{}/{}'.format(blob.bucket.name, blob.name)
    #end def

    def __str__(self):
        return 'gs://{}/{}'.format(self.blob.bucket.name, self.blob.name)
#end class


class HTTPURI(BaseURI):
    """
    Storage system for HTTP/HTTPS.
    """

    SUPPORTED_SCHEMES = set(['http', 'https'])
    """Supported schemes for :class:`HTTPURI`."""

    VALID_STORAGE_ARGS = ['params', 'headers', 'cookies', 'auth', 'timeout', 'allow_redirects', 'proxies', 'verify', 'stream', 'cert', 'method']
    """Keyword arguments passed to :func:`requests.request`."""

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        global requests

        if uri.scheme not in cls.SUPPORTED_SCHEMES: return None
        if requests is None: raise ImportError('You need to install requests package to handle {} URIs.'.format(uri.scheme))

        return HTTPURI(uri.geturl(), storage_args=storage_args)
    #end def

    def __init__(self, url, raise_for_status=True, method=None, storage_args={}):
        """
        :param str uri: HTTP URI.
        :param str raise_for_status: Raises a :exc:`requests.RequestException` when the response status code is not 2xx (i.e., calls :meth:`requests.Request.raise_for_status`)
        :param str method: Overrides the default method for all HTTP operations.
        :param dict storage_args: Keyword arguments that are passed to :func:`requests.request`
        """

        super(HTTPURI, self).__init__(storage_args=storage_args)
        self.url = url
        self.method = self.storage_args.pop('method', method)
        self.raise_for_status = self.storage_args.pop('raise_for_status', raise_for_status)
    #end def

    def get_content(self):
        r = requests.request(self.method if self.method else 'GET', self.url, **self.storage_args)
        if self.raise_for_status: r.raise_for_status()
        return r.content
    #end def

    def put_content(self, content):
        """
        Makes a ``PUT`` request with the content in the body.

        :raise: An :exc:`requests.RequestException` if it is not 2xx.
        """

        r = requests.request(self.method if self.method else 'PUT', self.url, data=content, **self.storage_args)
        if self.raise_for_status: r.raise_for_status()
    #end def

    def download_file(self, filename):
        kwargs = self.storage_args.copy()
        stream = kwargs.pop('stream', True)
        r = requests.request(self.method if self.method else 'GET', self.url, stream=stream, **kwargs)
        if self.raise_for_status: r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                f.write(chunk)
    #end def

    def upload_file(self, filename):
        with open(filename, 'rb') as f:
            r = requests.request(self.method if self.method else 'PUT', self.url, data=f, **self.storage_args)
        if self.raise_for_status: r.raise_for_status()
    #end def

    def exists(self):
        try:
            requests.head(self.url).raise_for_status()
            return True
        except requests.HTTPError: return False
    #end def

    def dir_exists(self):
        """
        Makes a ``HEAD`` requests to the URI.

        :returns: ``True`` if status code is 2xx.
        """

        r = requests.request(self.method if self.method else 'HEAD', self.url, **self.storage_args)
        try: r.raise_for_status()
        except Exception: return False

        return True
    #end def

    def make_dir(self):
        """Ignored."""
        pass

    def __str__(self):
        return self.url
#end class


class SNSURI(BaseURI):
    """
    Storage system for AWS Simple Notification Service."""

    SUPPORTED_SCHEMES = set(['sns'])
    """Supported schemes for :class:`SNSURI`."""

    VALID_STORAGE_ARGS = ['Subject', 'MessageAttributes', 'MessageStructure']
    """Keyword arguments passed to :meth:`SNS.Client.publish`."""

    sns_resource = None

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        if uri.scheme not in cls.SUPPORTED_SCHEMES: return None
        if boto3 is None: raise ImportError('You need to install boto3 package to handle {} URIs.'.format(uri.scheme))

        if cls.sns_resource is None: cls.sns_resource = boto3.resource('sns')

        return SNSURI(uri.netloc, uri.path, storage_args=storage_args)
    #end def

    def __init__(self, topic_name, region, storage_args={}):
        """
        :param str topic_name: Name of SNS topic for publishing; it can be either an ARN or just the topic name (thus defaulting to the current role's account)
        :param str region: AWS region of SNS topic (defaults to current role's region)
        :param dict storage_args: Keyword arguments that are passed to :meth:`SNS.Client.publish`
        """

        super(SNSURI, self).__init__(storage_args=storage_args)

        region = region.lstrip('/')
        if not region:
            region = boto3.session.Session().region_name

        topic = None

        if topic_name.startswith('arn:'):
            topic = self.sns_resource.Topic(topic_name)
        else:
            account_id = boto3.client('sts').get_caller_identity().get('Account')
            topic = self.sns_resource.Topic('arn:aws:sns:{}:{}:{}'.format(region, account_id, topic_name))
        #end if

        self.topic = topic
    #end def

    def get_content(self):
        """Not supported."""

        raise TypeError('SNSURI does not support reading.')
    #end def

    def put_content(self, content):
        """
        Publishes a message straight to SNS.

        :param bytes content: raw bytes content to publish, will decode to ``UTF-8`` if string is detected
        """
        if not isinstance(content, str):
            content = content.decode('utf-8')

        self.topic.publish(Message=content, **self.storage_args)
    #end def

    def download_file(self, filename):
        """Not supported."""

        raise TypeError('SNSURI does not support reading.')
    #end def

    def upload_file(self, filename):
        with open(filename, 'rb') as f:
            self.topic.publish(Message=f.read(), **self.storage_args)
    #end def

    def exists(self):
        """:returns: ``True`` if the SNS topic exists"""

        return self.topic.arn is not None
    #end def

    def dir_exists(self):
        """Not supported."""
        raise TypeError('SNSURI does not support directories.')

    def make_dir(self):
        raise TypeError('SNSURI does not support directories.')

    def __str__(self):
        return self.topic.arn
#end class


STORAGES = [FileURI, S3URI, GoogleCloudStorageURI, HTTPURI, SNSURI]
