"""
This module provides wrapper function for transparently handling files regardless of location (local, cloud, etc).
"""
__all__ = ['uri_open', 'uri_read', 'uri_dump', 'uri_exists', 'get_uri_metadata', 'uri_exists_wait', 'URIFileType', 'URIType', 'DirType']

# from contextlib import contextmanager
import gzip
from io import BytesIO, TextIOWrapper, FileIO, BufferedReader
import logging
import os
from tempfile import NamedTemporaryFile
import time

try: from urlparse import urlparse  # Python 2
except ImportError: from urllib.parse import urlparse  # Python 3

from .storages import STORAGES, URIBytesOutput

logger = logging.getLogger(__name__)


def uri_open(uri, mode='rb', auto_compress=True, in_memory=True, textio_args={}, storage_args={}):
    uri_obj = _get_uri_obj(uri, storage_args)

    if mode == 'rb': read_mode, binary_mode = True, True
    elif mode == 'r': read_mode, binary_mode = True, False
    elif mode == 'w': read_mode, binary_mode = False, False
    elif mode == 'wb': read_mode, binary_mode = False, True
    else: raise TypeError('`mode` cannot be "{}".'.format(mode))

    if read_mode:
        if in_memory:
            file_obj = BytesIO(uri_obj.get_content())
            setattr(file_obj, 'name', str(uri_obj))
        else:
            file_obj = _TemporaryURIFileIO(uri_obj=uri_obj, input_mode=True)
        #end if
    else:
        if in_memory: file_obj = URIBytesOutput(uri_obj)
        else:
            file_obj = _TemporaryURIFileIO(uri_obj=uri_obj, input_mode=False, pre_close_action=uri_obj.upload_file)
            setattr(file_obj, 'name', str(uri_obj))
        #end if
    #end if

    temp_name = getattr(file_obj, 'temp_name', None)

    if auto_compress:
        _, ext = os.path.splitext(uri)
        ext = ext.lower()
        if ext == '.gz': file_obj = gzip.GzipFile(fileobj=file_obj, mode='rb' if read_mode else 'wb')
    #end if

    if not binary_mode:
        textio_args.setdefault('encoding', 'utf-8')
        file_obj = TextIOWrapper(file_obj, **textio_args)
    #end if

    setattr(file_obj, 'temp_name', temp_name)

    return file_obj
#end def


def uri_read(*args, **kwargs):
    with uri_open(*args, **kwargs) as f:
        content = f.read()
    return content
#end def


def _get_uri_obj(uri, storage_args={}):
    uri_obj = None
    o = urlparse(uri)
    for storage in STORAGES:
        uri_obj = storage.parse_uri(o, storage_args=storage_args)
        if uri_obj is not None:
            break
    #end for
    if uri_obj is None:
        raise TypeError('<{}> is an unsupported URI.'.format(uri))

    return uri_obj
#end def


def uri_dump(uri, content, mode='wb', **kwargs):
    with uri_open(uri, mode=mode, **kwargs) as f:
        f.write(content)
#end def


def get_uri_metadata(uri, storage_args={}):
    uri_obj = _get_uri_obj(uri, storage_args)
    return uri_obj.get_metadata()
#end def


def uri_exists(uri, storage_args={}):
    uri_obj = _get_uri_obj(uri, storage_args)
    return uri_obj.exists()
#end def


def uri_exists_wait(uri, timeout=300, interval=5, storage_args={}):
    uri_obj = _get_uri_obj(uri, storage_args)
    start_time = time.time()
    while time.time() - start_time < timeout:
        if uri_obj.exists(uri): return True
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


class DirType(object):
    def __init__(self, mode='r', create=True):
        self.mode = mode
        self.create = create
    #end def

    def __call__(self, uri):
        if self.mode == 'r':
            if not os.path.isdir(uri): raise OSError('<{}> is not a valid directory path.'.format(uri))
        else:
            if not os.path.isdir(uri):
                os.makedirs(uri)
        #end if
        return uri
    #end def
#end class


class _TemporaryURIFileIO(FileIO):
    def __init__(self, uri_obj=None, input_mode=True, pre_close_action=None):
        with NamedTemporaryFile(delete=False) as f:
            temp_file = f.name

        if input_mode and uri_obj:
            uri_obj.download_file(temp_file)
        #end if

        self.uri_obj = uri_obj
        self.temp_file = temp_file
        self.pre_close_action = pre_close_action

        super(_TemporaryURIFileIO, self).__init__(temp_file, 'rb' if input_mode else 'wb')

        self.name = str(self.uri_obj)
    #end def

    @property
    def temp_name(self):
        return self.temp_file

    def close(self):
        super(_TemporaryURIFileIO, self).close()

        if self.pre_close_action: self.pre_close_action(self.temp_file)
        os.remove(self.temp_file)
    #end def
#end class
