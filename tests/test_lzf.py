import unittest

from yxdb._lzf import Lzf


class TestLzf(unittest.TestCase):
    def test_empty_input(self):
        in_bytes = memoryview(bytearray(0))
        out_bytes = memoryview(bytearray(0))
        lzf = Lzf(in_bytes, out_bytes)

        written = lzf.decompress(0)
        self.assertEqual(0, written)

    def test_output_array_is_too_small(self):
        in_bytes = memoryview(bytearray([0, 25]))
        out_bytes = memoryview(bytearray([]))
        lzf = Lzf(in_bytes, out_bytes)

        self.assertRaises(AttributeError, lambda: lzf.decompress(2))

    def test_small_control_values_do_simple_copies(self):
        in_bytes = memoryview(bytearray([4, 1, 2, 3, 4, 5]))
        out_bytes = memoryview(bytearray(5))
        lzf = Lzf(in_bytes, out_bytes)

        written = lzf.decompress(6)
        self.assertEqual(5, written)
        self.assertEqual(bytearray([1, 2, 3, 4, 5]), out_bytes)

    def test_multiple_small_control_values(self):
        in_bytes = memoryview(bytearray([2, 1, 2, 3, 1, 1, 2]))
        out_bytes = memoryview(bytearray(5))
        lzf = Lzf(in_bytes, out_bytes)

        written = lzf.decompress(7)
        self.assertEqual(5, written)
        self.assertEqual(bytearray([1, 2, 3, 1, 2]), out_bytes)

    def test_expand_large_control_values(self):
        in_bytes = memoryview(bytearray([2, 1, 2, 3, 32, 1]))
        out_bytes = memoryview(bytearray(6))
        lzf = Lzf(in_bytes, out_bytes)

        written = lzf.decompress(6)
        self.assertEqual(6, written)
        self.assertEqual(bytearray([1, 2, 3, 2, 3, 2]), out_bytes)

    def test_large_control_values_with_length_of_7(self):
        in_bytes = memoryview(bytearray([8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 224, 1, 8]))
        out_bytes = memoryview(bytearray(19))
        lzf = Lzf(in_bytes, out_bytes)

        written = lzf.decompress(13)
        self.assertEqual(19, written)
        self.assertEqual(bytearray([1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1]), out_bytes)

    def test_output_array_too_small_for_large_control_value(self):
        in_bytes = memoryview(bytearray([8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 224, 1, 8]))
        out_bytes = memoryview(bytearray(17))
        lzf = Lzf(in_bytes, out_bytes)

        self.assertRaises(AttributeError, lambda: lzf.decompress(13))

    def test_reset_lzf_and_start_again(self):
        in_bytes = memoryview(bytearray([4, 1, 2, 3, 4, 5]))
        out_bytes = memoryview(bytearray(5))
        lzf = Lzf(in_bytes, out_bytes)

        lzf.decompress(6)

        in_bytes[0] = 2
        in_bytes[1] = 6
        in_bytes[2] = 7
        in_bytes[3] = 8

        written = lzf.decompress(4)
        self.assertEqual(3, written)
        self.assertEqual(bytearray([6, 7, 8, 4, 5]), out_bytes)


if __name__ == '__main__':
    unittest.main()
