import unittest

from yxdb.spatial import to_geojson
from yxdb.yxdb_reader import YxdbReader


class TestSpatialReaders(unittest.TestCase):
    def test_point(self):
        expected = '{"type": "Point", "coordinates": [-96.679688, 37.230328]}'
        path = './test_files/point.yxdb'
        self._test_spatial(path, expected)

    def test_points(self):
        expected = '{"type": "MultiPoint", "coordinates": [[-113.730469, 7.885147], [-113.378906, 46.679594], [-100.019531, 40.178873], [-88.769531, 49.61071], [-85.957031, 12.039321]]}'
        path = './test_files/multi-point.yxdb'
        self._test_spatial(path, expected)

    def test_line(self):
        expected = '{"type": "LineString", "coordinates": [[-106.875, 42.293564], [-84.375, 41.244772], [-106.347656, 36.738884], [-85.253906, 35.173808], [-110.390625, 32.546813], [-89.472656, 29.22889]]}'
        path = './test_files/line.yxdb'
        self._test_spatial(path, expected)

    def test_lines(self):
        expected = '{"type": "MultiLineString", "coordinates": [[[-92.285156, 55.875311], [-74.355469, 53.225768], [-76.992188, 41.902277], [-76.992188, 29.382175], [-66.269531, 43.068888]], [[-108.984375, 43.197167], [-70.664063, 49.037868], [-97.558594, 25.799891], [-73.125, 21.616579], [-97.910156, 4.565474], [-81.386719, -3.513421]], [[-121.464844, 45.213004], [-109.6875, -0.175781]], [[-114.082031, 57.231503], [-107.753906, 55.677584], [-111.972656, 51.399206], [-120.9375, 54.470038], [-122.34375, 58.995311], [-115.136719, 62.103883], [-104.0625, 59.085739], [-101.777344, 51.944265], [-108.28125, 47.517201], [-123.222656, 50.176898]]]}'
        path = './test_files/multi-line.yxdb'
        self._test_spatial(path, expected)

    def _test_spatial(self, path, expected):
        yxdb = YxdbReader(path=path)
        while yxdb.next():
            spatial = yxdb.read_index(1)
            json_str = to_geojson(spatial)
            self.assertEqual(expected, json_str)

        yxdb.close()
