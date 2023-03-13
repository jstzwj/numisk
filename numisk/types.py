
from enum import Enum

class DtypeKind(Enum):
    boolean = 0
    int8 = 1
    int16 = 2
    int32 = 3
    int64 = 4
    uint8 = 5
    uint16 = 6
    uint32 = 7
    uint64 = 8
    float8 = 9
    float16 = 10
    float32 = 11
    float64 = 12
    complex128 = 13
    string = 14
    obj = 15

class Dtype(object):
    def __init__(self, base: DtypeKind) -> None:
        self.base = base