from .abstract_query import AbstractQuery
from abc import abstractmethod


class FilteredQuery(AbstractQuery):
    @abstractmethod
    def __init__(self):
        super().__init__()
        self._filter = lambda entry: True

    def where(self, column, condition):
        self._filter = lambda entry: condition(entry[column])
        return self
