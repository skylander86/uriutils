__all__ = ['STORAGES', 'URIBytesOutput']

from io import BytesIO
import os
import shutil

try:
    import boto3
    import botocore.exceptions
    s3_resource = boto3.resource('s3')
except ImportError: s3_resource = None

try:
    from google.cloud import storage as gcloud_storage
    import google.cloud.exceptions
    gs_client = gcloud_storage.Client()
except ImportError: gs_client = None

try: import requests
except ImportError: requests = None


class URIBytesOutput(BytesIO):
    """A BytesIO object that flushes content to the remote object on close."""

    def __init__(self, uri_obj):
        super(URIBytesOutput, self).__init__()
        self.uri_obj = uri_obj
    #end def

    def close(self):
        self.uri_obj.put_content(self.getvalue())
        super(URIBytesOutput, self).close()
    #end def

    @property
    def name(self):
        return str(self.uri_obj)
#end class


class BaseURI(object):
    SUPPORTED_SCHEMES = []
    VALID_STORAGE_ARGS = []

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        """Returns `None` if this storage system does not support :attr:`uri`."""
        raise NotImplementedError('`parse_uri` is not implemented for {}.'.format(type(cls).__name__))
    #end def

    def __init__(self, storage_args={}):
        self.storage_args = dict((k, v) for k, v in storage_args.items() if k in self.VALID_STORAGE_ARGS)
    #end def

    def get_content(self):
        """Returns the binary content stored in the URI for this object."""
        raise NotImplementedError('`get_content` is not implemented for {}.'.format(type(self).__name__))

    def put_content(self, content):
        """Returns a file-like object that allows writing to this URI."""
        raise NotImplementedError('`put_content` is not implemented for {}.'.format(type(self).__name__))

    def download_file(self, filename):
        """Download the binary content stored in the URI for this object directly to `filename`."""
        raise NotImplementedError('`download_file` is not implemented for {}.'.format(type(self).__name__))

    def upload_file(self, filename):
        """Download the binary content stored in the URI for this object directly to `filename`."""
        raise NotImplementedError('`upload_file` is not implemented for {}.'.format(type(self).__name__))

    def get_metadata(self): return {}

    def exists(self):
        """Check if the URI exists."""
        raise NotImplementedError('`exists` is not implemented for {}.'.format(type(self).__name__))

    def __str__(self):
        """Returns a nicely formed URI for this object."""
        raise NotImplementedError('`__str__` is not implemented for {}.'.format(type(self).__name__))
    #end def

    def __unicode__(self):
        return self.__str__()

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, str(self))
#end class


class FileURI(BaseURI):
    SUPPORTED_SCHEMES = set(['file', ''])

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
        with open(self.filepath, 'rb') as f:
            return f.read()
    #end def

    def put_content(self, content):
        with open(self.filepath, 'wb') as f:
            return f.write(content)

    def download_file(self, filename):
        shutil.copyfile(self.filepath, filename)
    #end def

    def upload_file(self, filename):
        shutil.copyfile(filename, self.filepath)

    def exists(self):
        return os.path.exists(self.filepath)

    def __str__(self):
        return self.filepath
#end class


class S3URI(BaseURI):
    SUPPORTED_SCHEMES = set(['s3'])
    VALID_STORAGE_ARGS = ['CacheControl', 'ContentDisposition', 'ContentEncoding', 'ContentLanguage', 'ContentLength', 'ContentMD5', 'ContentType', 'Expires', 'GrantFullControl', 'GrantRead', 'GrantReadACP', 'GrantWriteACP', 'Metadata', 'ServerSideEncryption', 'StorageClass', 'WebsiteRedirectLocation', 'SSECustomerAlgorithm', 'SSECustomerKey', 'SSEKMSKeyId', 'RequestPayer', 'Tagging']

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        if uri.scheme not in cls.SUPPORTED_SCHEMES: return None
        if s3_resource is None: raise ImportError('You need to install boto3 package to handle this {} URIs.'.format(uri.scheme))

        return S3URI(uri.netloc, uri.path.lstrip('/'), storage_args=storage_args)
    #end def

    def __init__(self, bucket, key, storage_args={}):
        super(S3URI, self).__init__(storage_args=storage_args)

        self.s3_object = s3_resource.Object(bucket, key)
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
        self.s3_object.load()
        return self.s3_object.metadata

    def exists(self):
        try:
            self.s3_object.load()
            return True
        except botocore.exceptions.ClientError: return False
    #end def

    def __str__(self):
        return 's3://{}/{}'.format(self.s3_object.bucket_name, self.s3_object.key)
#end class


class GoogleCloudStorageURI(BaseURI):
    SUPPORTED_SCHEMES = set(['gs', 'gcs'])
    VALID_STORAGE_ARGS = ['chunk_size', 'encryption_key']

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        if uri.scheme not in cls.SUPPORTED_SCHEMES: return None
        if gs_client is None: raise ImportError('You need to install google-cloud-storage package to handle this {} URIs.'.format(uri.scheme))

        return GoogleCloudStorageURI(uri.netloc, uri.path.lstrip('/'), storage_args=storage_args)
    #end def

    def __init__(self, bucket, key, storage_args={}):
        self.content_type = storage_args.get('content_type', 'application/octet-stream')
        self.content_encoding = storage_args.get('content_encoding', None)
        self.metadata = storage_args.get('metadata', {})
        self.metadata.update(storage_args.get('Metadata', {}))

        super(GoogleCloudStorageURI, self).__init__(storage_args=storage_args)

        self.blob = gs_client.bucket(bucket).blob(key, **self.storage_args)
    #end def

    def get_content(self):
        return self.blob.download_as_string()

    def put_content(self, content):
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
        self.blob.reload()
        return self.blob.metadata

    def exists(self):
        try:
            self.blob.reload()
            return True
        except google.cloud.exceptions.NotFound: return False
    #end def

    def __str__(self):
        return 'gs://{}/{}'.format(self.blob.bucket.name, self.blob.name)
#end class


class HTTPURI(BaseURI):
    SUPPORTED_SCHEMES = set(['http', 'https'])
    VALID_STORAGE_ARGS = ['params', 'headers', 'cookies', 'auth', 'timeout', 'allow_redirects', 'proxies', 'verify', 'stream', 'cert']

    @classmethod
    def parse_uri(cls, uri, storage_args={}):
        if uri.scheme not in cls.SUPPORTED_SCHEMES: return None
        if requests is None: raise ImportError('You need to install requests package to handle this {} URIs.'.format(uri.scheme))

        return HTTPURI(uri.geturl(), storage_args=storage_args)
    #end def

    def __init__(self, url, storage_args={}):
        super(HTTPURI, self).__init__(storage_args=storage_args)
        self.url = url
        self.method = self.storage_args.pop('method', None)
    #end def

    def get_content(self):
        r = requests.request(self.method if self.method else 'GET', self.url, **self.storage_args)
        return r.content
    #end def

    def put_content(self, content):
        requests.request(self.method if self.method else 'PUT', self.url, data=content, **self.storage_args)

    def download_file(self, filename):
        kwargs = self.storage_args.copy()
        stream = kwargs.pop('stream', True)
        r = requests.request(self.method if self.method else 'GET', self.url, stream=stream, **kwargs)
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                f.write(chunk)
    #end def

    def upload_file(self, filename):
        with open(filename, 'rb') as f:
            requests.request(self.method if self.method else 'PUT', self.url, data=f, **self.storage_args)
    #end def

    def exists(self):
        try:
            requests.head(self.url).raise_for_status()
            return True
        except requests.HTTPError: return False
    #end def

    def __str__(self):
        return self.url
#end class


STORAGES = [FileURI, S3URI, GoogleCloudStorageURI, HTTPURI]