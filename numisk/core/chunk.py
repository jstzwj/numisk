

import os
from typing import Literal
import struct
from ..version import NUMISK_PROTOCOL_VERSION


NUMISK_CHUNK_HEADER_SIZE = 16

class ChunkHeader(object):
    def __init__(self) -> None:
        self.version = None
        self.fixed = None
        self.compressed = None
        self.dtype = None
        self.item_num = None

    @classmethod
    def decode(cls, bytes: bytes) -> "ChunkHeader":
        ret = cls()
        items = struct.unpack("<I??xxII", bytes)
        ret.version, ret.fixed, ret.compressed, ret.dtype, ret.item_num = items
        return ret
    
    def encode(self) -> bytes:
        ret = struct.pack("<I??xxII", self.version, self.fixed, self.compressed, self.dtype, self.item_num)
        return ret

class Chunk(object):
    def __init__(self,
                chunkfile: str,
                mode: Literal["w", "r"]="r"
            ) -> None:
        # IO Params
        self.chunkfile = chunkfile
        self.mode = mode

        # Meta data
        self.header = None

        # data
        self._is_open = False
        self.raw_data = None
    
    
        # open
        if not self._is_open:
            self.open()
    
    def __enter__(self):
        # open
        if not self._is_open:
            self.open()
        return self

    def __exit__(self, exct_type, exce_value, traceback):
        self.close()

    def __del__(self):
        self.close()
    

    def open(self):
        # Read or create meta data
        if os.path.exists(self.chunkfile):
            self._read_chunk_meta(self.chunkfile)
        else:
            self.header = ChunkHeader()
            self.header.version = NUMISK_PROTOCOL_VERSION
            self.header.fixed = True
            self.header.compressed = True
            self.header.dtype = 0
            self.header.item_num = 0
            self._write_chunk_meta(self.chunkfile)
        
        # read item data
        
        self._is_open = True

    def close(self):
        pass
    
    def _read_chunk_meta(self, meta_path: str):
        with open(meta_path, "rb") as f:
            self.header = ChunkHeader.decode(f.read(NUMISK_CHUNK_HEADER_SIZE))

    def _write_chunk_meta(self, meta_path: str):
        with open(meta_path, "wb") as f:
            f.write(self.header.encode())
    