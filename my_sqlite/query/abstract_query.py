from abc import ABC, abstractmethod
from config.config import Config
import os


class AbstractQuery(ABC):
    _database_path = Config.database_path
    _file_extension = Config.table_filename_extension
    _sep = Config.separator
    _linesep = os.linesep

    def __init__(self):
        self._main_table = None

    @abstractmethod
    def run(self):
        pass
