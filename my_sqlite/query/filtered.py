import collections
import itertools
from abc import abstractmethod

from my_sqlite.conversion import converted
from my_sqlite.errors import AmbiguousColumnNameError, NoSuchColumnError
from my_sqlite.query.abstract import AbstractQuery


class FilteredQuery(AbstractQuery):
    @abstractmethod
    def __init__(self):
        super().__init__()
        self._key_mapper = KeyMapper(self)
        self._where_filter = lambda row: True

    def where(self, column, *, condition):
        [key] = self._key_mapper.map(column)
        self._where_filter = lambda row: condition(converted(row[key.table][key.column]))
        return self

    def _get_full_key_set(self):
        return (Key(*key)
                for key in itertools.chain.from_iterable(itertools.product((table.index,), range(len(table.header_map)))
                                                         for table in self.get_tables_in_query_order()))


class KeyMapper:
    def __init__(self, query):
        self.query: FilteredQuery = query

    def map(self, *keys):
        return tuple(self._map(key) for key in keys)

    def _map(self, key):
        key_parts = key.split('.', maxsplit=1)
        if len(key_parts) == 1:
            return self._map_one_part(key)
        else:  # len == 2
            return self._map_two_parts(key_parts)

    def _map_one_part(self, key):
        matches = tuple((table.index, table.header_map[key])
                        for table in self.query.table_map.values() if key in table.header_map)
        if len(matches) > 1:
            raise AmbiguousColumnNameError(key)
        if not matches:
            raise NoSuchColumnError(key)
        return Key(*matches[0])

    def _map_two_parts(self, parts):
        try:
            table = self.query.table_map[parts[0]]
            return Key(table=table.index, column=table.header_map[parts[1]])
        except KeyError:
            raise NoSuchColumnError('.'.join(parts))


Key = collections.namedtuple('Key', ['table', 'column'])