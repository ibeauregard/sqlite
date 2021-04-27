from abc import ABC, abstractmethod
from config.config import Config


class AbstractQuery(ABC):
    database_path = Config.database_path
    file_extension = Config.table_filename_extension

    def __init__(self):
        self._main_table = None

    @abstractmethod
    def run(self):
        pass
