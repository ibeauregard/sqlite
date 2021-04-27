from abc import ABC, abstractmethod


class AbstractQuery(ABC):
    file_extension = '.csv'

    def __init__(self):
        self._main_table = None

    @abstractmethod
    def run(self):
        pass
