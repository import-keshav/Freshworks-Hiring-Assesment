import fcntl
import os

from . import config
from .functions import DataStore


def get_file_name():
    ''' Creates a unique file name for datastore by appending
        epoch timestamp to the file name
    '''
    import uuid
    uniq_append_string = uuid.uuid4().hex
    return "LOCAL_STORAGE_{}".format(uniq_append_string)


def get_instance(file_name=None):
    '''
    Args:
        file_name str: file path.

    Returns:
        obj Datastore: Returns a Datastore class object.
    '''
    if file_name is None:
        file_name = get_file_name()
    full_file_name = f"{config.LOCAL_STORAGE_PREPEND_PATH}/{file_name}"
    file_descriptor = os.open(full_file_name, os.O_CREAT | os.O_RDWR)

    '''
        Try to acquire file lock. 
        If file is already locked, `fcntl` raises BlockingIOError which can be used to handle exceptions.
    '''
    try:
        print(f"Acquiring file lock on {file_name}")
        fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise BlockingIOError(f"Resource '{file_name}' is already locked'")
    except Exception:
        raise
    else:
        print(f"File lock acquired on {file_name}")

    '''
        File lock acquired. Put an empty json string ahead in the file as python mmap doesn't support an empty file to be mmap-ed. 
    '''
    if not os.path.isfile(full_file_name) or os.fstat(file_descriptor).st_size == 0:
        with open(full_file_name, 'ab') as f:
            string = "{}"
            f.write(bytes(string.encode('ascii')))
    return DataStore(file_descriptor)

__all__ = ['get_instance']
