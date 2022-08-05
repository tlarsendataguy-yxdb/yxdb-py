import unittest

from yxdb._extractors import *


class TestExtractors(unittest.TestCase):
    def test_extract_int16(self):
        extract = new_int16_extractor(2)
        result = extract(memview([0, 0, 10, 0, 0, 0]))
        self.assertEqual(10, result)

    def test_extract_null_int16(self):
        extract = new_int16_extractor(2)
        result = extract(memview([0, 0, 10, 0, 1, 0]))
        self.assertEqual(None, result)

    def test_extract_int32(self):
        extract = new_int32_extractor(3)
        result = extract(memview([0, 0, 0, 10, 0, 0, 0, 0]))
        self.assertEqual(10, result)

    def test_extract_null_int32(self):
        extract = new_int32_extractor(3)
        result = extract(memview([0, 0, 0, 10, 0, 0, 0, 1]))
        self.assertEqual(None, result)

    def test_extract_int64(self):
        extract = new_int64_extractor(4)
        result = extract(memview([0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0]))
        self.assertEqual(10, result)

    def test_extract_null_int64(self):
        extract = new_int64_extractor(4)
        result = extract(memview([0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 1]))
        self.assertEqual(None, result)

    def test_extract_bool(self):
        extract = new_bool_extractor(4)
        result = extract(memview([0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]))
        self.assertEqual(True, result)

        result = extract(memview([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
        self.assertEqual(False, result)

    def test_extract_null_bool(self):
        extract = new_bool_extractor(4)
        result = extract(memview([0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0]))
        self.assertEqual(None, result)

    def test_extract_byte(self):
        extract = new_byte_extractor(4)
        result = extract(memview([0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0]))
        self.assertEqual(10, result)

    def test_extract_null_byte(self):
        extract = new_byte_extractor(4)
        result = extract(memview([0, 0, 0, 0, 10, 1, 0, 0, 0, 0, 0, 0, 0]))
        self.assertEqual(None, result)

    def test_extract_float(self):
        extract = new_float_extractor(4)
        result = extract(memview([0, 0, 0, 0, 205, 206, 140, 63, 0, 0, 0, 0, 0]))
        self.assertEqual(struct.unpack('f', bytes([205, 206, 140, 63])), result)

    def test_extract_null_float(self):
        extract = new_float_extractor(4)
        result = extract(memview([0, 0, 0, 0, 205, 206, 140, 63, 1, 0, 0, 0, 0]))
        self.assertEqual(None, result)

    def test_extract_double(self):
        extract = new_double_extractor(4)
        result = extract(memview([0, 0, 0, 0, 154, 155, 155, 155, 155, 155, 241, 63, 0]))
        self.assertEqual(struct.unpack('d', bytes([154, 155, 155, 155, 155, 155, 241, 63])), result)

    def test_extract_null_double(self):
        extract = new_double_extractor(4)
        result = extract(memview([0, 0, 0, 0, 154, 155, 155, 155, 155, 155, 241, 63, 1]))
        self.assertEqual(None, result)

    def test_extract_date(self):
        extract = new_date_extractor(4)
        result = extract(memview([0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 0]))
        self.assertEqual(datetime.datetime(2021, 1, 1), result)

    def test_extract_null_date(self):
        extract = new_date_extractor(4)
        result = extract(memview([0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 1]))
        self.assertEqual(None, result)


def memview(data):
    return memoryview(bytes(data))


if __name__ == '__main__':
    unittest.main()
