import xml.etree.ElementTree as ET
from io import BytesIO
from typing import List

from yxdb._buffered_record_reader import BufferedRecordReader
from yxdb._metainfo_field import MetaInfoField
from yxdb._yxdb_record import YxdbRecord
from yxdb.yxdb_field import YxdbField


class YxdbReader:
    """
    YxdbReader contains the public API for reading YXDB files

    Instantiate YxdbReader with either a string containing the path to a YXDB file,
    or with a BytesIO object containing an in-memory stream of a YXDB file.

    Use the next() method to iterate records.

    Access fields using the following methods:
        * read_index(): read by field index
        * read_name(): read by the name of the field

    The value returned will be a data type appropriate for that field.

    Use the list_fields() method to obtain the list of fields in the YXDB file.
    """

    def __init__(self, *args, **kwargs):
        """
        Instantiate a YXDB reader with 1 of the following parameters:
            * path: a string containing the path to a YXDB file
            * stream: a BytesIO object that streams a YXDB file
        """

        stream: BytesIO = kwargs.get('stream', None)
        path: str = kwargs.get('path', None)
        if stream is None and path is None:
            raise TypeError("either 'path' or 'stream' must be provided")
        if stream is None:
            if not isinstance(path, str):
                raise TypeError("'path' must be a string")
            stream = open(path, 'rb')
        if path is None:
            path = ''

        self._stream = stream
        self._path = path
        self._fields: List[MetaInfoField] = []
        self.num_records = 0
        self._meta_info_size = 0
        self.meta_info_str = ''
        self._record: YxdbRecord = None
        self._record_reader: BufferedRecordReader = None

        self._load_header_and_meta_info()

    def next(self) -> bool:
        """Returns True if a record is available and False if the end of the file is reached."""

        return self._record_reader.next_record()

    def read_index(self, index: int):
        """Returns the value in a field, specified by the field's index."""

        return self._record.extract_from_index(index, self._record_reader.record_buffer)

    def read_name(self, name: str):
        """Returns the value in a field, specified by the field's name"""

        return self._record.extract_from_name(name, self._record_reader.record_buffer)

    def list_fields(self) -> List[YxdbField]:
        """Provides the list of fields in the YXDB file"""

        return self._record.fields

    def close(self):
        """Closes the YXDB stream early."""

        self._stream.close()

    def _load_header_and_meta_info(self):
        header = self._get_header()
        self.num_records = int.from_bytes(header[104:108], 'little')
        self._meta_info_size = int.from_bytes(header[80:94], 'little')
        self._load_meta_info()
        self._record = YxdbRecord(self._fields)
        self._record_reader = BufferedRecordReader(self._stream, self._record.fixed_size, self._record.has_var, self.num_records)

    def _get_header(self) -> memoryview:
        buffer = memoryview(bytearray(512))
        read = self._stream.readinto(buffer)
        if read < 512:
            self._close_stream_and_raise()
        return buffer

    def _load_meta_info(self):
        length = (self._meta_info_size * 2) - 2
        meta_info_bytes = self._stream.read(length)
        self._stream.read(2)
        if len(meta_info_bytes) != length:
            self._close_stream_and_raise()
        self.meta_info_str = str(meta_info_bytes, "utf_16_le")
        self._get_fields()

    def _get_fields(self):
        root = ET.fromstring(self.meta_info_str)
        for field in root.iter(tag="Field"):
            name = _parse_string(field.get("name"))
            data_type = _parse_string(field.get("type"))
            size = _parse_int(field.get("size"))
            scale = _parse_int(field.get("scale"))

            self._fields.append(MetaInfoField(name, data_type, size, scale))

    def _close_stream_and_raise(self):
        self._stream.close()
        raise IOError("file is not a valid YXDB")


def _parse_int(value) -> int:
    if value is None:
        return 0
    return int(value)


def _parse_string(value) -> str:
    if value is None:
        raise IOError("YXDB metadata is invalid")
    return value
