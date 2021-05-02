import heapq
import itertools

from .filtered import FilteredQuery
from ..conversion import converted


class Select(FilteredQuery):
    def __init__(self):
        super().__init__()
        self._on_filter = lambda row: True
        self._select_keys = None
        self._order_key = None
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
        key1, key2 = self._key_mapper.map(*join_keys)
        self._on_filter = lambda row: row[key1.table][key1.column] == row[key2.table][key2.column]

    def select(self, columns):
        self._select_keys = tuple(itertools.chain.from_iterable(
            self._key_mapper.map(col) if col != '*' else self._get_full_key_set() for col in columns))
        return self

    def order_by(self, column, *, ascending=True):
        [key] = self._key_mapper.map(column)

        def order_key(row):
            value = converted(row[key.table][key.column])
            return (value == '' if ascending else value != ''), value
        self._order_key = order_key
        self._order_ascending = ascending
        return self

    def limit(self, limit):
        self._limit = limit if limit >= 0 else None
        return self

    def run(self):
        filtered_rows = (row for row in self._get_rows() if self._on_filter(row) and self._where_filter(row))
        return ((row[table][column] for table, column in self._select_keys) if self._select_keys
                else itertools.chain(*row)
                for row in self._order_and_limit()(filtered_rows))

    def _get_rows(self):
        tables = []
        for table in self.get_tables_in_query_order():
            with open(table.path) as table_file:
                rows = self._parse_table(table_file)
            tables.append(rows)
        return itertools.product(*tables)

    def _order_and_limit(self):
        if self._order_key is not None:
            if self._limit:
                return lambda rows: \
                    (heapq.nsmallest if self._order_ascending else heapq.nlargest)(self._limit,
                                                                                   rows,
                                                                                   key=self._order_key)
            else:
                return lambda rows: sorted(rows, key=self._order_key, reverse=not self._order_ascending)
        else:
            return lambda rows: itertools.islice(rows, self._limit)
