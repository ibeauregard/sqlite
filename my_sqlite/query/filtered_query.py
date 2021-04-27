from .abstract_query import AbstractQuery
from abc import abstractmethod


class FilteredQuery(AbstractQuery):
    @abstractmethod
    def __init__(self):
        super().__init__()
        self._filter = None

    def where(self, column_name, condition):
        self._filter = lambda entry: condition(entry[column_name])
        return self
