from typing import Dict, List, Callable

from yxdb import _extractors
from yxdb._metainfo_field import MetaInfoField
from yxdb.yxdb_field import YxdbField, DataType


class YxdbRecord:
    def __init__(self, fields: List[MetaInfoField]):
        self._name_to_index: Dict[str, int] = {}
        self.fields: List[YxdbField] = []
        self._extractors: List[Callable] = []
        self.has_var = False
        self.fixed_size = 0

        self._initialize(fields)

    def extract_from_index(self, index: int, buffer: memoryview):
        return self._extractors[index](buffer)

    def extract_from_name(self, name: str, buffer: memoryview):
        index = self._name_to_index[name]
        return self._extractors[index](buffer)

    def _initialize(self, fields: List[MetaInfoField]):
        start_at = 0
        for field in fields:
            if field.data_type == 'Int16':
                self._add_extractor(field.name, DataType.LONG, _extractors.new_int16_extractor(start_at))
                start_at += 3
                continue
            if field.data_type == 'Int32':
                self._add_extractor(field.name, DataType.LONG, _extractors.new_int32_extractor(start_at))
                start_at += 5
                continue
            if field.data_type == 'Int64':
                self._add_extractor(field.name, DataType.LONG, _extractors.new_int64_extractor(start_at))
                start_at += 9
                continue
            if field.data_type == 'Float':
                self._add_extractor(field.name, DataType.DOUBLE, _extractors.new_float_extractor(start_at))
                start_at += 5
                continue
            if field.data_type == 'Double':
                self._add_extractor(field.name, DataType.DOUBLE, _extractors.new_double_extractor(start_at))
                start_at += 9
                continue
            if field.data_type == 'FixedDecimal':
                size = field.size
                self._add_extractor(field.name, DataType.DOUBLE, _extractors.new_fixed_decimal_extractor(start_at, size))
                start_at += size + 1
                continue
            if field.data_type == 'String':
                size = field.size
                self._add_extractor(field.name, DataType.STRING, _extractors.new_string_extractor(start_at, size))
                start_at += size + 1
                continue
            if field.data_type == 'WString':
                size = field.size
                self._add_extractor(field.name, DataType.STRING, _extractors.new_wstring_extractor(start_at, size))
                start_at += (size * 2) + 1
                continue
            if field.data_type == "V_String":
                self._add_extractor(field.name, DataType.STRING, _extractors.new_v_string_extractor(start_at))
                start_at += 4
                self.has_var = True
                continue
            if field.data_type == "V_WString":
                self._add_extractor(field.name, DataType.STRING, _extractors.new_v_wstring_extractor(start_at))
                start_at += 4
                self.has_var = True
                continue
            if field.data_type == "Date":
                self._add_extractor(field.name, DataType.DATE, _extractors.new_date_extractor(start_at))
                start_at += 11
                continue
            if field.data_type == "DateTime":
                self._add_extractor(field.name, DataType.DATE, _extractors.new_date_time_extractor(start_at))
                start_at += 20
                continue
            if field.data_type == "Bool":
                self._add_extractor(field.name, DataType.BOOLEAN, _extractors.new_bool_extractor(start_at))
                start_at += 1
                continue
            if field.data_type == "Byte":
                self._add_extractor(field.name, DataType.BYTE, _extractors.new_byte_extractor(start_at))
                start_at += 2
                continue
            if field.data_type == "Blob" or field.data_type == "SpatialObj":
                self._add_extractor(field.name, DataType.BLOB, _extractors.new_blob_extractor(start_at))
                start_at += 4
                self.has_var = True
                continue
            raise NameError
        self.fixed_size = start_at

    def _add_extractor(self, name: str, data_type: DataType, extractor):
        self._add_field_name_to_index_map(name, data_type)
        self._extractors.append(extractor)

    def _add_field_name_to_index_map(self, name: str, data_type: DataType):
        index = len(self.fields)
        self.fields.append(YxdbField(name, data_type))
        self._name_to_index[name] = index
        return index
