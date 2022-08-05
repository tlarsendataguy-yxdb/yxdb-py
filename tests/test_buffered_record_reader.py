import unittest

from yxdb._buffered_record_reader import BufferedRecordReader
from yxdb._utility import memview


class TestBufferedRecordReader(unittest.TestCase):
    def test_lots_of_records(self):
        reader = generate_reader("./test_files/LotsOfRecords.yxdb", 5, False)
        records_read = 0
        while reader.next_record():
            records_read += 1
            self.assertEqual(records_read, int.from_bytes(reader.record_buffer[0:4], 'little'))
        self.assertEqual(100000, records_read)

    def test_very_long_field(self):
        reader = generate_reader("./test_files/VeryLongField.yxdb", 6, True)
        records_read = 0
        while reader.next_record():
            records_read += 1
            self.assertEqual(records_read, reader.record_buffer[0])
        self.assertEqual(3, records_read)


def generate_reader(path: str, fixed_len: int, has_var_fields: bool):
    stream = open(path, "rb")
    header = memview(512)
    stream.readinto(header)
    meta_info_size = int.from_bytes(header[80:84], 'little') * 2
    total_records = int.from_bytes(header[104:108], 'little')
    stream.seek(512 + meta_info_size)
    return BufferedRecordReader(stream, fixed_len, has_var_fields, total_records)


