import collections
from abc import ABC, abstractmethod
from pathlib import Path
from config.config import Config
from ..errors import NoSuchTableError
import os


class AbstractQuery(ABC):
    _database_path = Config.database_path
    _file_extension = Config.table_filename_extension
    _sep = Config.column_separator
    _linesep = os.linesep

    def __init__(self):
        self._tables = []
        self.table_map = {}
        self.header_maps = []

    @abstractmethod
    def run(self):
        pass

    def append_table(self, table):
        table_path = Path(self._database_path) / f'{table}{self._file_extension}'
        if not table_path.is_file():
            raise NoSuchTableError(table)
        self._tables.append(Table(table, path=table_path))
        with open(table_path) as table_file:
            headers = self.strip_and_split(next(table_file))
        self.table_map[table] = max(self.table_map.values(), default=-1) + 1
        self.header_maps.append({header: i for i, header in enumerate(headers)})

    @classmethod
    def _parse_table(cls, table):
        header_map = {header: i for i, header in enumerate(cls.strip_and_split(next(table)))}
        entries = list(map(cls.strip_and_split, table))
        return header_map, entries

    @classmethod
    def _join_entries(cls, headers, entries):
        return f"{cls._sep.join(headers)}{cls._linesep}" \
               f"{os.linesep.join(cls._sep.join(entry) for entry in entries)}{cls._linesep}"

    @classmethod
    def strip_and_split(cls, line):
        return line.strip().split(cls._sep)


Table = collections.namedtuple('Table', ['name', 'path'])
