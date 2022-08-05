import struct


def new_bool_extractor(start: int):
    def e(buffer: memoryview):
        value = buffer[start]
        if value == 2:
            return None
        return value == 1
    return e


def new_byte_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+1] == 1:
            return None
        return buffer[start]
    return e


def new_int16_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+2] == 1:
            return None
        return int.from_bytes(buffer[start:start+2], 'little')
    return e


def new_int32_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+4] == 1:
            return None
        return int.from_bytes(buffer[start:start+4], 'little')
    return e


def new_int64_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+8] == 1:
            return None
        return int.from_bytes(buffer[start:start+8], 'little')
    return e


def new_fixed_decimal_extractor(start: int, length: int):
    def e(buffer: memoryview):
        if buffer[start+length] == 1:
            return None
        value = _get_string(buffer, start, length, 1)
        return float(value)
    return e


def new_float_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+4] == 1:
            return None
        return struct.unpack_from('f', buffer, start)
    return e


def new_double_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+8] == 1:
            return None
        return struct.unpack_from('d', buffer, start)
    return e


def _get_string(buffer: memoryview, start: int, field_len: int, char_size: int):
    end = _get_end_of_string_pos(buffer, start, field_len, char_size)
    if char_size == 1:
        return str(buffer[start:end].tobytes(), 'utf_8')
    else:
        return str(buffer[start:end].tobytes(), 'utf_16_le')


def _get_end_of_string_pos(buffer: memoryview, start: int, field_len: int, char_size: int):
    field_to = start + (field_len * char_size)
    str_len = 0
    i = 0
    while i < field_to:
        if buffer[i] == 0 and buffer[i + (char_size-1)] == 0:
            break
        str_len += 1
    return start + (str_len * char_size)
