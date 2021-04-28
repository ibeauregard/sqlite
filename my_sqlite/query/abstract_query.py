from abc import ABC, abstractmethod
from pathlib import Path
from config.config import Config
import os


class AbstractQuery(ABC):
    _database_path = Config.database_path
    _file_extension = Config.table_filename_extension
    _sep = Config.column_separator
    _linesep = os.linesep

    def __init__(self):
        self._main_table = None
        self._main_table_path = None

    @property
    def main_table(self):
        return self._main_table_path

    @main_table.setter
    def main_table(self, table):
        self._main_table = table
        self._main_table_path = f'{Path(self._database_path) / table}{self._file_extension}'

    @abstractmethod
    def run(self):
        pass
