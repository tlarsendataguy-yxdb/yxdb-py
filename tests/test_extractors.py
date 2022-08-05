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

    def test_extract_datetime(self):
        extract = new_date_time_extractor(4)
        result = extract(memview([0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48,
                                  53, 0]))
        self.assertEqual(datetime.datetime(2021, 1, 2, 3, 4, 5), result)

    def test_extract_null_datetime(self):
        extract = new_date_time_extractor(4)
        result = extract(memview([0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48,
                                  53, 1]))
        self.assertEqual(None, result)

    def test_extract_string(self):
        extract = new_string_extractor(2, 15)
        result = extract(memview([0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0]))
        self.assertEqual("hello world!", result)

    def test_extract_full_string(self):
        extract = new_string_extractor(2, 5)
        result = extract(memview([0, 0, 104, 101, 108, 108, 111, 0]))
        self.assertEqual("hello", result)

    def test_extract_null_string(self):
        extract = new_string_extractor(2, 5)
        result = extract(memview([0, 0, 104, 101, 108, 108, 111, 1]))
        self.assertEqual(None, result)

    def test_extract_empty_string(self):
        extract = new_string_extractor(2, 5)
        result = extract(memview([0, 0, 0, 101, 108, 108, 111, 0]))
        self.assertEqual("", result)

    def test_extract_fixed_decimal(self):
        extract = new_fixed_decimal_extractor(2, 10)
        result = extract(memview([0, 0, 49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0]))
        self.assertEqual(123.45, result)

    def test_extract_null_fixed_decimal(self):
        extract = new_fixed_decimal_extractor(2, 10)
        result = extract(memview([0, 0, 49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 1]))
        self.assertEqual(None, result)

    def test_extract_wstring(self):
        extract = new_wstring_extractor(2, 15)
        result = extract(memview([0, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0,
                                  100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 0]))
        self.assertEqual("hello world", result)

    def test_extract_null_wstring(self):
        extract = new_wstring_extractor(2, 15)
        result = extract(memview([0, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0,
                                  100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 1]))
        self.assertEqual(None, result)

    def test_extract_empty_wstring(self):
        extract = new_wstring_extractor(2, 15)
        result = extract(memview([0, 0, 0, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0,
                                  100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 0]))
        self.assertEqual("", result)


def memview(data):
    return memoryview(bytes(data))


if __name__ == '__main__':
    unittest.main()
