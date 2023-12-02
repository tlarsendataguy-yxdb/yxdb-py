import json
import struct
from typing import List


_bytes_per_point = 16


def to_geojson(value: bytes) -> str:
    """
    to_geojson translates SpatialObj fields into GeoJSON.

    Alteryx stores spatial objects in a binary format.
    This function reads the binary format and converts it to a GeoJSON string.

    :param value: The object read from a SpatialObj field
    :return: A GeoJSON string representing the spatial object
    :raises TypeError: The blob is not a valid spatial object
    """
    if value is None:
        return None
    if len(value) < 20:
        raise TypeError('blob is not a spatial object')
    obj_type = int.from_bytes(value[0:4], byteorder='little')
    if obj_type == 8:
        return _parse_points(value)
    if obj_type == 3:
        return _parse_lines(value)
    if obj_type == 5:
        return _parse_poly(value)
    raise TypeError("blob is not a spatial object")


def _parse_points(value: bytes) -> str:
    total_points = int.from_bytes(value[36:40], byteorder='little')
    if total_points == 1:
        return _parse_single_point(value)
    return _parse_multi_point(value)


def _parse_lines(value: bytes) -> str:
    lines = _parse_multipoint_objects(value)

    if len(lines) == 1:
        return _geojson('LineString', lines[0])

    return _geojson('MultiLineString', lines)


def _parse_poly(value: bytes) -> str:
    poly = _parse_multipoint_objects(value)

    if len(poly) == 1:
        return _geojson('Polygon', poly)

    return _geojson('MultiPolygon', [poly])


def _parse_multipoint_objects(value: bytes) -> List:
    ending_indices = _get_ending_indices(value)

    i = 48 + (len(ending_indices) * 4) - 4
    objects = []
    for end_at in ending_indices:
        line = []
        while i < end_at:
            line.append(_get_coord_at(value, i))
            i += _bytes_per_point
        objects.append(line)
    return objects


def _parse_single_point(value: bytes) -> str:
    lng = struct.unpack('d', value[40:48])[0]
    lat = struct.unpack('d', value[48:56])[0]
    return _geojson('Point', [lng, lat])


def _parse_multi_point(value: bytes) -> str:
    points = []
    i = 40
    while i < len(value):
        points.append(_get_coord_at(value, i))
        i += _bytes_per_point
    return _geojson('MultiPoint', points)


def _get_coord_at(value: bytes, at: int) -> List[float]:
    lng = struct.unpack('d', value[at:at + 8])[0]
    lat = struct.unpack('d', value[at+8:at + _bytes_per_point])[0]
    return [lng, lat]


def _get_ending_indices(value: bytes) -> List[int]:
    total_lines = int.from_bytes(value[36:40], byteorder='little')
    total_points = int.from_bytes(value[40:48], byteorder='little')
    ending_indices = []
    i = 48
    start_at = 48 + ((total_lines - 1) * 4)
    for line in range(total_lines - 1):
        ending_point = int.from_bytes(value[i:i + 4], byteorder='little')
        ending_index = (ending_point * _bytes_per_point) + start_at
        ending_indices.append(ending_index)
        i += 4
    ending_indices.append((total_points * _bytes_per_point) + start_at)
    return ending_indices


def _geojson(obj_type: str, obj) -> str:
    obj = {
        "type": obj_type,
        "coordinates": obj
    }
    return json.dumps(obj)
