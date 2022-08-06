## yxdb-py

yxdb-py is a library for reading YXDB files into Python.

install using `pip install yxdb`

The library does not have external dependencies and is a pure Python solution.

The public API is contained in the YxdbReader class. Instantiate YxdbReader with the following constructors:
* `YxdbReader(path=str)` - load from a file
* `YxdbReader(stream=BytesIO)` - load from an in-memory stream

Iterate through the records in the file using the `next()` method in a while loop:

```
while reader.next():
    # do something
```

Fields can be access via the `read_index()` and `read_name()` methods on the YxdbReader class.

The list of fields in the YXDB file can be access via the `list_fields()` method.
