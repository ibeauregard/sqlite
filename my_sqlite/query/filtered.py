import collections
import itertools
from abc import abstractmethod

from my_sqlite.conversion import converted
from my_sqlite.errors import AmbiguousColumnNameError, NoSuchColumnError, NoSuchTableError
from my_sqlite.query.abstract import AbstractQuery


class FilteredQuery(AbstractQuery):
    @abstractmethod
    def __init__(self):
        super().__init__()
        self._where_filter = lambda row: True

    def where(self, column, *, condition):
        [key] = self._map_keys(column)
        self._where_filter = lambda row: condition(converted(row[key.table][key.column]))
        return self

    def _map_keys(self, *keys):
        return tuple(self._map_key(key) for key in keys)

    def _map_key(self, key):
        key_parts = key.split('.', maxsplit=1)
        if len(key_parts) == 1:
            return self._map_one_part_key(key)
        else:  # len == 2
            return self._map_two_part_key(key_parts)

    def _map_one_part_key(self, key):
        lower_key = key.lower()
        matches = tuple((table.index, table.header_map[lower_key])
                        for table in self.table_map.values() if lower_key in table.header_map)
        if len(matches) > 1:
            raise AmbiguousColumnNameError(key)
        if not matches:
            raise NoSuchColumnError(key)
        return Key(*matches[0])

    def _map_two_part_key(self, parts):
        try:
            table = self.table_map[parts[0]]
            return Key(table=table.index, column=table.header_map[parts[1].lower()])
        except KeyError:
            raise NoSuchColumnError('.'.join(parts))

    def _get_key_set(self, table):
        try:
            return (Key(*key) for key in
                    (self._get_full_key_set() if table is None else self._get_one_table_key_set(self.table_map[table])))
        except KeyError:
            raise NoSuchTableError(table)

    def _get_full_key_set(self):
        return itertools.chain.from_iterable(self._get_one_table_key_set(table)
                                             for table in self.get_tables_in_query_order())

    @staticmethod
    def _get_one_table_key_set(table):
        return itertools.product((table.index,), range(len(table.header_map)))


Key = collections.namedtuple('Key', ['table', 'column'])
