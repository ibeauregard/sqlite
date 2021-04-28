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

    @classmethod
    def _parse_table(cls, table):
        column_headers = cls._strip_and_split(next(table))
        entries = (dict(zip(column_headers, entry)) for entry in map(cls._strip_and_split, table))
        return column_headers, entries

    @classmethod
    def _strip_and_split(cls, line):
        return line.strip().split(cls._sep)

