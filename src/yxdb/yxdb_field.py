from enum import Enum


class DataType(Enum):
    BLOB = 1
    BOOLEAN = 2
    BYTE = 3
    DATE = 4
    DOUBLE = 5
    LONG = 6
    STRING = 7


class YxdbField:
    def __init__(self, name: str, data_type: DataType):
        self.name = name
        self.data_type = data_type
