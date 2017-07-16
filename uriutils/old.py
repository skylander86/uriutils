"""
This module provides wrapper function for transparently handling files regardless of location (local, cloud, etc).
"""
__all__ = ['uri_open', 'uri_to_tempfile', 'uri_read', 'uri_dump', 'uri_exists', 'get_uri_metadata', 'uri_exists_wait', 'URIFileType', 'URIType']

from contextlib import contextmanager
import gzip
from io import BytesIO, TextIOWrapper
import logging
import os
from shutil import copyfileobj
from tempfile import NamedTemporaryFile
import time

try: from urlparse import urlparse  # Python 2
except ImportError: from urllib.parse import urlparse  # Python 3

from .storages import STORAGES

logger = logging.getLogger(__name__)


def uri_open(uri, mode='rb', encoding='utf-8', use_gzip='auto', io_args={}, urifs_args={}):
    _, ext = os.path.splitext(o.path)
    use_gzip = use_gzip == 'always' or use_gzip is True or (ext in ['.gz'] and use_gzip == 'auto')
    binary_mode = 'b' in mode
    read_mode = 'r' in mode

    urifs_args = _filter_urifsargs(urifs_args, o.scheme)

    if read_mode:
        if o.scheme == 's3':
            r = s3_client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'), **urifs_args)
            fileobj = BytesIO(r['Body'].read())  # future: Add support for local temp file

        elif o.scheme == 'gs':
            fileobj = BytesIO(gs_client.bucket(o.netloc).blob(o.path.lstrip('/'), **urifs_args).download_as_string())  # future: Add support for local temp file

        elif o.scheme in ['http', 'https']:
            r = requests.get(uri, **urifs_args) 
            fileobj = BytesIO(r.content)

            if not binary_mode and not use_gzip: encoding = r.encoding

        elif not o.scheme or o.scheme == 'file':
            fpath = os.path.join(o.netloc, o.path.lstrip('/')).rstrip('/') if o.netloc else o.path
            fileobj = open(fpath, 'rb', **urifs_args)
        #end if

        if not hasattr(fileobj, 'name'): setattr(fileobj, 'name', uri)

    else:  # write mode
        if o.scheme == 's3':
            if use_gzip: urifs_args['ContentEncoding'] = 'gzip'
            fileobj = S3URIWriter(bucket=o.netloc, key=o.path.lstrip('/'), **urifs_args)

        elif o.scheme == 'gs':
            if use_gzip: urifs_args['content_type'] = 'application/gzip'
            fileobj = GCSURIWriter(bucket=o.netloc, key=o.path.lstrip('/'), **urifs_args)

        elif o.scheme in ['http', 'https']:
            raise OSError('Write mode not supported for {}.'.format(o.scheme.upper()))

        elif not o.scheme or o.scheme == 'file':
            fpath = os.path.join(o.netloc, o.path.lstrip('/')).rstrip('/') if o.netloc else o.path
            fileobj = open(fpath, 'wb')
        #end if
    #end if

    if use_gzip: fileobj = gzip.GzipFile(fileobj=fileobj, mode='rb' if read_mode else 'wb')
    if not binary_mode: fileobj = TextIOWrapper(fileobj, encoding=encoding, **io_args)

    return fileobj
#end def


def _filter_urifsargs(urifs_args, scheme):
    if scheme == 's3':
        valid_keys = ['Bucket', 'IfMatch', 'IfModifiedSince', 'IfNoneMatch', 'IfUnmodifiedSince', 'Key', 'Range', 'ResponseCacheControl', 'ResponseContentDisposition', 'ResponseContentEncoding', 'ResponseContentLanguage', 'ResponseContentType', 'ResponseExpires', 'VersionId', 'SSECustomerAlgorithm', 'SSECustomerKey', 'RequestPayer', 'PartNumber']
    elif scheme == 'gs':
        valid_keys = ['chunk_size', 'encryption_key']
    elif scheme in ['http', 'https']:
        valid_keys = ['params', 'headers', 'cookies', 'auth', 'timeout', 'allow_redirects', 'proxies', 'verify', 'stream', 'cert']
    else:
        valid_keys = []

    return dict((k, v) for k, v in urifs_args.items() if k in valid_keys)
#end def


@contextmanager
def uri_to_tempfile(uri, *, delete=True, **kwargs):
    with uri_open(uri, mode='rb', use_gzip=False) as f_uri:
        with NamedTemporaryFile(mode='wb', prefix='uri.', delete=False) as f_temp:
            copyfileobj(f_uri, f_temp)
            f_temp_name = f_temp.name
        #end with
    #end with
    logger.debug('URI <{}> downloaded to temporary file <{}> ({} bytes).'.format(uri, f_temp_name, os.path.getsize(f_temp_name)))

    f = uri_open(f_temp_name, **kwargs)

    yield f

    f.close()
    if delete: os.remove(f_temp_name)
#end def


def uri_read(uri, mode='rb', encoding='utf-8', use_gzip='auto', io_args={}, urifs_args={}):
    with uri_open(uri, mode=mode, encoding=encoding, use_gzip=use_gzip, io_args=io_args, urifs_args=urifs_args) as f:
        content = f.read()
    return content
#end def


def uri_dump(uri, content, mode='wb', encoding='utf-8', use_gzip='auto', io_args={}, urifs_args={}):
    with uri_open(uri, mode=mode, encoding=encoding, use_gzip=use_gzip, io_args=io_args, urifs_args=urifs_args) as f:
        f.write(content)
#end def


def get_uri_metadata(uri):
    o = _check_uri_support(uri, supported_schemes=['s3'])

    if o.scheme == 's3':
        response = s3_client.head_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
        return response['Metadata']
    #end if

    return {}
#end def


def uri_exists(uri):
    o = _check_uri_support(uri)

    if o.scheme == 's3':
        try:
            s3_client.head_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
            return True
        except ClientError: return False

    elif o.scheme == 'gs':
        try:
            blob = gs_client.bucket(o.netloc).blob(o.pth.lstrip('/'))
            return blob is not None
        except Exception: return False

    elif o.scheme in ['http', 'https']:
        try:
            requests.head(uri).raise_for_status()
            return True
        except requests.HTTPError: return False

    elif not o.scheme or o.scheme == 'file':
        fpath = os.path.join(o.netloc, o.path.lstrip('/')).rstrip('/') if o.netloc else o.path
        return os.path.exists(fpath)
    #end if

    return False
#end def


def uri_exists_wait(uri, timeout=300, interval=5):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if uri_exists(uri): return True
        time.sleep(interval)
    #end while

    if uri_exists(uri): return True

    return False
#end def


class URIFileType(object):
    def __init__(self, mode='rb', **kwargs):
        self.kwargs = kwargs
        self.kwargs['mode'] = mode
    #end def

    def __call__(self, uri):
        return uri_open(uri, **self.kwargs)
#end class


class URIType(object):
    def __call__(self, uri):
        o = urlparse(uri)
        return o
    #end def
#end class


class BucketWriter(BytesIO):
    def __init__(self, scheme, bucket, key, **urifs_args):
        super(BucketWriter, self).__init__()
        self.scheme = scheme
        self.bucket = bucket
        self.key = key
        self.urifs_args = urifs_args
    #end def

    @property
    def name(self): return '{}://{}/{}'.format(self.scheme, self.bucket, self.key)
#end class


class S3URIWriter(BucketWriter):
    def __init__(self, bucket, key, **urifs_args):
        super(S3URIWriter, self).__init__('s3', bucket, key, **urifs_args)
    #end def

    def close(self):
        s3_client.put_object(Bucket=self.bucket, Key=self.key, Body=self.getvalue(), **self.urifs_args)
        super(S3URIWriter, self).close()
    #end def
#end class


class GCSURIWriter(BucketWriter):
    def __init__(self, bucket, key, **urifs_args):
        super(GCSURIWriter, self).__init__('gs', bucket, key, **urifs_args)
        self.urifs_args.setdefault('content_type', 'application/octet-stream')
    #end def

    def close(self):
        gs_client.bucket(self.bucket).blob(self.key).upload_from_string(self.getvalue(), **self.urifs_args)
        super(GCSURIWriter, self).close()
    #end def
#end class
