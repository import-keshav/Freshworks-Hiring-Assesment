# Freshworks-Hiring-Assesment
Freshworks Hiring Assesment


##### File based Key-Value datastore

   Supports basic CRD (Create, Read, Delete)

**Functionalities:**
  1. It can be initialized using an optional file path. If one is not provided, it will reliably create itself using `uuid`.
  2. Key string capped at 32 characters and Value must be a JSON object capped at 16KB. (These values can be changed in config.py)
  3. Every key supports setting a Time-To-Live property when it is created. This property is optional. If provided, it will be evaluated as an integer defining the number of seconds. Once the Time-To-Live for a key has expired, the key will no longer be available for Read or Delete operations.
  4. Only one process can access the datastore (local file) at a time.
  5. Thread safe.


**Usage:**

###### Creating an instance
```
import key_value_data_store
ds_instance = key_value_data_store.get_instance()
```

Note: If `file_path` is provided in the `get_instance()` call, it will obtain lock on that file using `fcntl`. If object is created for the same file path twice, `BlockingIOError` is thrown.

###### Creating an data
```
data_key = 'test_key'
data_value = {"value": 1}  # must be a JSON
time_to_live = 5*1000  # in milliseconds
ds_instance.create(data_key, data_value, ttl=time_to_live)
```

###### Retrieving data
```
retrieve_key = 'test_key'
ds_instance.get(retrieve_key)   # returns {"value": 1} if retrieved within 5 seconds else ValueError
```

###### Deleting data
```
key_to_delete = 'test_key'
ds_instance.delete(key_to_delete)  # key-value is removed from the datastore
```

###### Delete all data
```
ds_instance.delete_all()
```
