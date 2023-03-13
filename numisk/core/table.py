import json
import os
import time
from typing import Dict, List, Literal

from .column import Column

TABLE_META_FILE = "NUMDISK_TABLE_META.numdisk"
TABLE_LOCK_FILE = "NUMDISK_TABLE_META.numdisk.lock"

class Table(object):
    def __init__(self, rootdir: str, mode: Literal["w", "r"]="r") -> None:
        # IO params
        self.rootdir = rootdir
        self.mode = mode

        # meta data
        self._column_names: List[str] = []
        # data
        self._is_open: bool = False
        self._columns: Dict[str, Column] = {}

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
    
    @property
    def columns(self) -> List[str]:
        return self._column_names
    
    def open(self):
        # Check Root Dir
        self._check_root_dir(rootdir=self.rootdir)
        # If mode is write, add a write lock
        if self.mode == "w":
            lock_status = self._lock_dir(self.rootdir)
            if not lock_status:
                raise ValueError("This table is used by another process.")
        elif self.mode == "r":
            lock_status = self._check_lock(self.rootdir)
            if lock_status:
                raise ValueError("This table is used by another process.")
        else:
            raise Exception("Unknown io mode.")
        
        # Read or create meta data
        meta_path = os.path.join(self.rootdir, TABLE_META_FILE)
        if os.path.exists(meta_path):
            self._read_table_meta(meta_path)
        else:
            self._create_table_meta(meta_path)

        # Init All Data
        for col_name in self._column_names:
            self._columns.append(Column(rootdir=self.rootdir, name=col_name))
        
        self._is_open = True

    def close(self):
        if self.mode == "w" and self._check_lock(self.rootdir):
            self._unlock_dir(self.rootdir)
        self._is_open = False

    def _check_root_dir(self, rootdir: str):
        if not os.path.exists(rootdir):
            os.makedirs(rootdir, exist_ok=True)
        else:
            if not os.path.isdir(rootdir):
                raise ValueError("rootdir is not a numdisk table dir.")
    
    def _check_lock(self, dirpath: str) -> bool:
        lock_file = os.path.join(dirpath, TABLE_LOCK_FILE)
        return os.path.exists(lock_file)

    def _lock_dir(self, dirpath: str) -> bool:
        lock_file = os.path.join(dirpath, TABLE_LOCK_FILE)
        if not os.path.exists(lock_file):
            lock_content = {"lock_time": time.time()}
            with open(lock_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(lock_content))
            return True
        else:
            return False
    
    def _unlock_dir(self, dirpath: str) -> bool:
        lock_file = os.path.join(dirpath, TABLE_LOCK_FILE)
        if os.path.exists(lock_file):
            os.remove(lock_file)
            return True
        else:
            return False

        
    def _create_table_meta(self, meta_path: str):
        meta = {
            "_column_names": self._column_names,
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(meta))
    
    def _read_table_meta(self, meta_path: str):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.loads(f.read())
        self._column_names = meta["_column_names"]

    def _write_table_meta(self):
        pass

    
    def __getitem__(self, key):
        pass

    def __setitem__(self, key, value):
        if isinstance(key, str):
            pass
