import datetime
import struct
import unittest

from yxdb._metainfo_field import MetaInfoField
from yxdb._yxdb_record import YxdbRecord
from yxdb.yxdb_field import DataType


class TestYxdbRecord(unittest.TestCase):
    def test_int16_record(self):
        record = load_record_with_value_column("Int16", 2)
        source = memview([23, 0, 0])

        self.assertEqual(1, len(record.fields))
        self.assertEqual(DataType.LONG, record.fields[0].data_type)
        self.assertEqual("value", record.fields[0].name)
        self.assertEqual(23, record.extract_from_index(0, source))
        self.assertEqual(23, record.extract_from_name("value", source))
        self.assertEqual(3, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_int32_record(self):
        record = load_record_with_value_column("Int32", 5)
        source = memview([23, 0, 0, 0, 0])

        self.assertEqual(23, record.extract_from_name("value", source))
        self.assertEqual(5, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_int64_record(self):
        record = load_record_with_value_column("Int64", 5)
        source = memview([23, 0, 0, 0, 0, 0, 0, 0, 0])

        self.assertEqual(23, record.extract_from_name("value", source))
        self.assertEqual(9, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_float_record(self):
        record = load_record_with_value_column("Float", 5)
        source = memview([205, 206, 140, 63, 0])
        expected = struct.unpack('f', source[:4].tobytes())[0]

        self.assertEqual(1, len(record.fields))
        self.assertEqual(DataType.DOUBLE, record.fields[0].data_type)
        self.assertEqual("value", record.fields[0].name)
        self.assertEqual(expected, record.extract_from_index(0, source))
        self.assertEqual(expected, record.extract_from_name("value", source))
        self.assertEqual(5, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_double_record(self):
        record = load_record_with_value_column("Double", 9)
        source = memview([154, 155, 155, 155, 155, 155, 241, 63, 0])
        expected = struct.unpack('d', source[:8].tobytes())[0]

        self.assertEqual(expected, record.extract_from_index(0, source))
        self.assertEqual(9, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_fixed_decimal_record(self):
        record = load_record_with_value_column("FixedDecimal", 10)
        source = memview([49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0])

        self.assertEqual(123.45, record.extract_from_name("value", source))
        self.assertEqual(11, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_string_record(self):
        record = load_record_with_value_column("String", 15)
        source = memview([104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0])

        self.assertEqual(1, len(record.fields))
        self.assertEqual(DataType.STRING, record.fields[0].data_type)
        self.assertEqual("value", record.fields[0].name)
        self.assertEqual("hello world!", record.extract_from_index(0, source))
        self.assertEqual("hello world!", record.extract_from_name("value", source))
        self.assertEqual(16, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_wstring_record(self):
        record = load_record_with_value_column("WString", 15)
        source = memview([104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 33, 0,
                          0, 0, 23, 0, 77, 0, 0])

        self.assertEqual("hello world!", record.extract_from_name("value", source))
        self.assertEqual(31, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_v_string_record(self):
        record = load_record_with_value_column("V_String", 15)
        source = memview([0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8])

        self.assertEqual("", record.extract_from_name("value", source))
        self.assertEqual(4, record.fixed_size)
        self.assertEqual(True, record.has_var)

    def test_v_wstring_record(self):
        record = load_record_with_value_column("V_WString", 15)
        source = memview([0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8])

        self.assertEqual("", record.extract_from_name("value", source))
        self.assertEqual(4, record.fixed_size)
        self.assertEqual(True, record.has_var)

    def test_date_record(self):
        record = load_record_with_value_column("Date", 10)
        source = memview([50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 0])

        self.assertEqual(1, len(record.fields))
        self.assertEqual(DataType.DATE, record.fields[0].data_type)
        self.assertEqual("value", record.fields[0].name)
        self.assertEqual(datetime.datetime(2021, 1, 1), record.extract_from_index(0, source))
        self.assertEqual(datetime.datetime(2021, 1, 1), record.extract_from_name("value", source))
        self.assertEqual(11, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_datetime_record(self):
        record = load_record_with_value_column("DateTime", 19)
        source = memview([50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48, 53, 0])

        self.assertEqual(datetime.datetime(2021, 1, 2, 3, 4, 5), record.extract_from_name("value", source))
        self.assertEqual(20, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_bool_record(self):
        record = load_record_with_value_column("Bool", 1)
        source = memview([1])

        self.assertEqual(1, len(record.fields))
        self.assertEqual(DataType.BOOLEAN, record.fields[0].data_type)
        self.assertEqual("value", record.fields[0].name)
        self.assertEqual(True, record.extract_from_index(0, source))
        self.assertEqual(True, record.extract_from_name("value", source))
        self.assertEqual(1, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_byte_record(self):
        record = load_record_with_value_column("Byte", 1)
        source = memview([23, 0])

        self.assertEqual(1, len(record.fields))
        self.assertEqual(DataType.BYTE, record.fields[0].data_type)
        self.assertEqual("value", record.fields[0].name)
        self.assertEqual(23, record.extract_from_index(0, source))
        self.assertEqual(23, record.extract_from_name("value", source))
        self.assertEqual(2, record.fixed_size)
        self.assertEqual(False, record.has_var)

    def test_blob_record(self):
        record = load_record_with_value_column("Blob", 100)
        source = memview([0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8])

        self.assertEqual(1, len(record.fields))
        self.assertEqual(DataType.BLOB, record.fields[0].data_type)
        self.assertEqual("value", record.fields[0].name)
        self.assertEqual(b'', record.extract_from_index(0, source))
        self.assertEqual(b'', record.extract_from_name("value", source))
        self.assertEqual(4, record.fixed_size)
        self.assertEqual(True, record.has_var)

    def test_spatial_obj_record(self):
        record = load_record_with_value_column("SpatialObj", 100)
        source = memview([0, 0, 0, 0, 4, 0, 0, 0, 1,2,3,4,5,6,7,8])

        self.assertEqual(b'', record.extract_from_name("value", source))
        self.assertEqual(4, record.fixed_size)
        self.assertEqual(True, record.has_var)


def load_record_with_value_column(data_type, size):
    return YxdbRecord([MetaInfoField("value", data_type, size, 0)])


def memview(data):
    return memoryview(bytes(data))


if __name__ == '__main__':
    unittest.main()
