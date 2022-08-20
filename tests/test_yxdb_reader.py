import datetime
import traceback
import unittest

from yxdb.yxdb_reader import YxdbReader


class TestYxdbReader(unittest.TestCase):
    def test_get_reader(self):
        path = "./test_files/AllNormalFields.yxdb"
        yxdb = YxdbReader(path=path)
        self.assertEqual(1, yxdb.num_records)
        self.assertNotEqual(None, yxdb.meta_info_str)
        self.assertEqual(16, len(yxdb.list_fields()))

        read = 0
        while yxdb.next():
            self.assertEqual(1, yxdb.read_index(0))
            self.assertEqual(1, yxdb.read_name("ByteField"))
            self.assertEqual(True, yxdb.read_index(1))
            self.assertEqual(True, yxdb.read_name("BoolField"))
            self.assertEqual(16, yxdb.read_index(2))
            self.assertEqual(16, yxdb.read_name("Int16Field"))
            self.assertEqual(32, yxdb.read_index(3))
            self.assertEqual(32, yxdb.read_name("Int32Field"))
            self.assertEqual(64, yxdb.read_index(4))
            self.assertEqual(64, yxdb.read_name("Int64Field"))
            self.assertEqual(123.45, yxdb.read_index(5))
            self.assertEqual(123.45, yxdb.read_name("FixedDecimalField"))
            self.assertEqual(0.12345, yxdb.read_index(7))
            self.assertEqual(0.12345, yxdb.read_name("DoubleField"))
            self.assertEqual("A", yxdb.read_index(8))
            self.assertEqual("A", yxdb.read_name("StringField"))
            self.assertEqual("AB", yxdb.read_index(9))
            self.assertEqual("AB", yxdb.read_name("WStringField"))
            self.assertEqual("ABC", yxdb.read_index(10))
            self.assertEqual("ABC", yxdb.read_name("V_StringShortField"))
            self.assertEqual("B" * 500, yxdb.read_index(11))
            self.assertEqual("B" * 500, yxdb.read_name("V_StringLongField"))
            self.assertEqual("XZY", yxdb.read_index(12))
            self.assertEqual("XZY", yxdb.read_name("V_WStringShortField"))
            self.assertEqual("W" * 500, yxdb.read_index(13))
            self.assertEqual("W" * 500, yxdb.read_name("V_WStringLongField"))
            self.assertEqual(datetime.datetime(2020, 1, 1), yxdb.read_index(14))
            self.assertEqual(datetime.datetime(2020, 1, 1), yxdb.read_name("DateField"))
            self.assertEqual(datetime.datetime(2020, 2, 3, 4, 5, 6), yxdb.read_index(15))
            self.assertEqual(datetime.datetime(2020, 2, 3, 4, 5, 6), yxdb.read_name("DateTimeField"))

            read += 1

        self.assertEqual(1, read)
        yxdb.close()

    def test_lots_of_records(self):
        path = "./test_files/LotsOfRecords.yxdb"
        yxdb = YxdbReader(path=path)

        total = 0
        while yxdb.next():
            total += yxdb.read_index(0)

        self.assertEqual(5000050000, total)
        yxdb.close()

    def test_load_reader_from_stream(self):
        stream = open("./test_files/LotsOfRecords.yxdb", 'rb')
        yxdb = YxdbReader(stream=stream)

        total = 0
        while yxdb.next():
            total += yxdb.read_index(0)

        self.assertEqual(5000050000, total)
        yxdb.close()

    def test_tutorial_data(self):
        path = "./test_files/TutorialData.yxdb"
        yxdb = YxdbReader(path=path)
        mr_count = 0
        while yxdb.next():
            if yxdb.read_name("Prefix") == "Mr":
                mr_count += 1
        self.assertEqual(4068, mr_count)

    def test_invalid_file(self):
        try:
            path = "./test_files/invalid.txt"
            YxdbReader(path=path)
        except Exception as e:
            self.assertEqual("file is not a valid YXDB format", str(e))
            traceback.print_exc()

    def test_small_invalid_file(self):
        try:
            path = "./test_files/invalidSmall.txt"
            YxdbReader(path=path)
        except Exception as e:
            self.assertEqual("file is not a valid YXDB format", str(e))
            traceback.print_exc()

    def test_invalid_field_index(self):
        try:
            path = "./test_files/TutorialData.yxdb"
            yxdb = YxdbReader(path=path)
            yxdb.next()
            yxdb.read_index(1000)
        except Exception as e:
            self.assertEqual("index 1000 is not a valid field index", str(e))
            traceback.print_exc()

    def test_invalid_field_name(self):
        try:
            path = "./test_files/TutorialData.yxdb"
            yxdb = YxdbReader(path=path)
            yxdb.next()
            yxdb.read_name("invalid field")
        except Exception as e:
            self.assertEqual("'invalid field' is not a valid field name", str(e))
            traceback.print_exc()


if __name__ == '__main__':
    unittest.main()
