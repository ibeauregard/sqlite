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
        self.table_map = {}

    @abstractmethod
    def run(self):
        pass

    def append_table(self, table):
        table_path = Path(self._database_path) / f'{table}{self._file_extension}'
        if not table_path.is_file():
            raise NoSuchTableError(table)
        with open(table_path) as table_file:
            headers = self.strip_and_split(next(table_file))
        self.table_map[table] = Table(index=len(self.table_map),
                                      path=table_path,
                                      header_map={header: i for i, header in enumerate(headers)})

    @classmethod
    def _parse_table(cls, table):
        next(table)  # skip header
        entries = list(map(cls.strip_and_split, table))
        return entries

    def _serialize_table(self, entries):
        header_map = next(iter(self.table_map.values())).header_map
        return f"{self._sep.join(header_map)}{self._linesep}" + self._serialize_rows(entries)

    @classmethod
    def _serialize_rows(cls, entries):
        return f"{os.linesep.join(cls._sep.join(entry) for entry in entries)}{cls._linesep}"

    @classmethod
    def strip_and_split(cls, line):
        return line.strip().split(cls._sep)


Table = collections.namedtuple('Table', ['index', 'path', 'header_map'])
