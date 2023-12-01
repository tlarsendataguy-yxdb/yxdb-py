import json
import struct
from typing import List


_bytes_per_point = 16


def to_geojson(value: bytes) -> str:
    obj_type = int.from_bytes(value[0:4], byteorder='little')
    if obj_type == 8:
        return _parse_points(value)
    if obj_type == 3:
        return _parse_lines(value)


def _parse_points(value: bytes) -> str:
    total_points = int.from_bytes(value[36:40], byteorder='little')
    if total_points == 1:
        return _parse_single_point(value)
    return _parse_multi_point(value)


def _parse_lines(value: bytes) -> str:
    ending_indices = _get_ending_indices(value)

    i = 48 + (len(ending_indices) * 4) - 4
    lines = []
    for end_at in ending_indices:
        line = []
        while i < end_at:
            line.append(_get_coord_at(value, i))
            i += _bytes_per_point
        lines.append(line)

    obj_label = 'MultiLineString'

    total_lines = len(ending_indices)
    if total_lines == 1:
        obj_label = 'LineString'
        lines = lines[0]

    obj = {
        "type": obj_label,
        "coordinates": lines
    }
    return json.dumps(obj)


def _parse_single_point(value: bytes) -> str:
    lng = struct.unpack('d', value[40:48])[0]
    lat = struct.unpack('d', value[48:56])[0]
    obj = {
        "type": "Point",
        "coordinates": [lng, lat]
    }
    return json.dumps(obj)


def _parse_multi_point(value: bytes) -> str:
    points = []
    i = 40
    while i < len(value):
        points.append(_get_coord_at(value, i))
        i += _bytes_per_point
    obj = {
        "type": "MultiPoint",
        "coordinates": points
    }
    return json.dumps(obj)


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
