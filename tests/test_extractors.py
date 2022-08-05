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


def memview(data):
    return memoryview(bytes(data))


if __name__ == '__main__':
    unittest.main()
