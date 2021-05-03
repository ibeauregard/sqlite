import itertools
import re

from .filtered import FilteredQuery
from my_sqlite.operator import operator
from ..conversion import converted


class Select(FilteredQuery):
    def __init__(self):
        super().__init__()
        self._on_filter = lambda row: True
        self._select_keys = None
        self._order_keys = []
        self._order_ascending = True
        self._limit = None

    def from_(self, table):
        self.append_table(table)
        return self

    def join(self, table, *, on):
        self.append_table(table)
        self._on(on)
        return self

    def _on(self, join_keys):
        key1, key2 = self._map_keys(*join_keys)
        self._on_filter = lambda row: row[key1.table][key1.column] == row[key2.table][key2.column]

    def select(self, columns):
        key_groups = []
        for column in columns:
            matches = re.match(r'(|(?P<table>.+)\.)(?=\*$)', column)
            key_groups.append(self._map_keys(column) if matches is None
                              else self._get_key_set(matches.groupdict()['table']))
        self._select_keys = tuple(itertools.chain.from_iterable(key_groups))
        return self

    def order_by(self, ordering_terms):
        columns, ascendings = zip(*ordering_terms)
        self._order_keys = tuple(zip(self._map_keys(*columns), map(operator.not_, ascendings)))
        return self

    def limit(self, limit):
        self._limit = limit if limit >= 0 else None
        return self

    def run(self):
        filtered_rows = (row for row in self._get_rows() if self._on_filter(row) and self._where_filter(row))
        return ((row[table][column] for table, column in self._select_keys) if self._select_keys
                else itertools.chain(*row)
                for row in self._order_and_limit(list(filtered_rows)))

    def _get_rows(self):
        tables = []
        for table in self.get_tables_in_query_order():
            with open(table.path) as table_file:
                rows = self._parse_table(table_file)
            tables.append(rows)
        return itertools.product(*tables)

    def _order_and_limit(self, rows):
        for key, reverse in reversed(self._order_keys):
            def sort_key(row):
                value = converted(row[key.table][key.column])
                return (value != '' if reverse else value == ''), value
            rows.sort(key=sort_key, reverse=reverse)
        return itertools.islice(rows, self._limit)
