import collections
from abc import ABC, abstractmethod
from pathlib import Path
from config.config import Config
from ..errors import check_existence
import os


class AbstractQuery(ABC):
    _database_path = Config.database_path
    _file_extension = Config.table_filename_extension
    _sep = Config.column_separator
    _linesep = os.linesep

    def __init__(self):
        self._tables = []

    @abstractmethod
    def run(self):
        pass

    @check_existence
    def append_table(self, table):
        self._tables.append(Table(table, path=self.path_from_table_name(table)))

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

    @classmethod
    def path_from_table_name(cls, table):
        return Path(cls._database_path) / f'{table}{cls._file_extension}'


Table = collections.namedtuple('Table', ['name', 'path'])
