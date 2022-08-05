import datetime
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
        return struct.unpack('f', buffer[start:start+4])[0]
    return e


def new_double_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+8] == 1:
            return None
        return struct.unpack('d', buffer[start:start+8])[0]
    return e


def new_date_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+10] == 1:
            return None
        return _parse_date(buffer, start, 10, "%Y-%m-%d")
    return e


def new_date_time_extractor(start: int):
    def e(buffer: memoryview):
        if buffer[start+19] == 1:
            return None
        return _parse_date(buffer, start, 19, "%Y-%m-%d %H:%M:%S")
    return e


def new_string_extractor(start: int, field_len: int):
    def e(buffer: memoryview):
        if buffer[start+field_len] == 1:
            return None
        return _get_string(buffer, start, field_len, 1)
    return e


def new_wstring_extractor(start: int, field_len: int):
    def e(buffer: memoryview):
        if buffer[start+(field_len*2)] == 1:
            return None
        return _get_string(buffer, start, field_len, 2)
    return e


def new_v_string_extractor(start: int):
    def e(buffer: memoryview):
        blob = _parse_blob(buffer, start)
        if blob is None:
            return None
        return str(blob, "utf_8")
    return e


def new_v_wstring_extractor(start: int):
    def e(buffer: memoryview):
        blob = _parse_blob(buffer, start)
        if blob is None:
            return None
        return str(blob, "utf_16_le")
    return e


def new_blob_extractor(start: int):
    def e(buffer: memoryview):
        return _parse_blob(buffer, start)
    return e


def _parse_date(buffer: memoryview, start: int, length: int, fmt: str):
    value = str(buffer[start:start+length].tobytes(), 'utf_8')
    return datetime.datetime.strptime(value, fmt)


def _get_string(buffer: memoryview, start: int, field_len: int, char_size: int):
    end = _get_end_of_string_pos(buffer, start, field_len, char_size)
    if char_size == 1:
        return str(buffer[start:end].tobytes(), 'utf_8')
    else:
        return str(buffer[start:end].tobytes(), 'utf_16_le')


def _get_end_of_string_pos(buffer: memoryview, start: int, field_len: int, char_size: int):
    field_to = start + (field_len * char_size)
    str_len = 0
    i = start
    while i < field_to:
        if buffer[i] == 0 and buffer[i + (char_size-1)] == 0:
            break
        str_len += 1
        i += char_size
    return start + (str_len * char_size)


def _parse_blob(buffer: memoryview, start: int) -> bytes:
    fixed_portion = int.from_bytes(buffer[start:start+4], 'little')
    if fixed_portion == 0:
        return bytearray(0)
    if fixed_portion == 1:
        return None

    if _is_tiny(fixed_portion):
        return _get_tiny_blob(start, buffer)

    block_start = start + (fixed_portion & 0x7fffffff)
    block_first_byte = buffer[block_start]
    if _is_small_block(block_first_byte):
        return _get_small_blob(buffer, block_start)
    else:
        return _get_normal_blob(buffer, block_start)


def _is_tiny(fixed_portion: int) -> bool:
    return fixed_portion & 0x80000000 == 0 and fixed_portion & 0x30000000 != 0


def _get_tiny_blob(start: int, buffer: memoryview) -> bytes:
    value = int.from_bytes(buffer[start:start+4], 'little')
    length = value >> 28
    end = start + length
    return buffer[start:end].tobytes()


def _is_small_block(value) -> bool:
    return (value & 1) == 1


def _get_small_blob(buffer: memoryview, block_start: int) -> bytes:
    first_byte = buffer[block_start]
    blob_len = first_byte >> 1
    blob_start = block_start + 1
    blob_end = blob_start + blob_len
    return buffer[blob_start:blob_end].tobytes()


def _get_normal_blob(buffer: memoryview, block_start: int):
    blob_len = int(int.from_bytes(buffer[block_start:block_start+4], 'little') / 2)
    blob_start = block_start + 4
    blob_end = blob_start + blob_len
    return buffer[blob_start:blob_end].tobytes()
