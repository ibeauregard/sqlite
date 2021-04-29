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
        self._table = None
        self._table_path = None

    @abstractmethod
    def run(self):
        pass

    @property
    def table(self):
        return self._table_path

    @table.setter
    @check_existence
    def table(self, table):
        self._table = table
        self._table_path = str(self.path_from_table_name(table))

    @classmethod
    def _parse_table(cls, table):
        header_map = {header: i for i, header in enumerate(cls.strip_and_split(next(table)))}
        entries = map(cls.strip_and_split, table)
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
