import collections
import functools
import heapq
import itertools

from .abstract import AbstractQuery
from ..conversion import converted
from ..errors import NoSuchColumnError, AmbiguousColumnNameError, check_existence


def update_maps(method):
    @functools.wraps(method)
    def wrapper(query, table_name, *args, **kwargs):
        return_value = method(query, table_name, *args, **kwargs)
        with open(query.path_from_table_name(table_name)) as table:
            headers = query.strip_and_split(next(table))
        query.table_map[table_name] = max(query.table_map.values(), default=-1) + 1
        query.header_maps.append({header: i for i, header in enumerate(headers)})
        return return_value
    return wrapper


class Select(AbstractQuery):
    def __init__(self):
        super().__init__()
        self._key_mapper = KeyMapper(self)
        self.table_map = {}
        self.header_maps = []
        self._on_filter = lambda row: True
        self._where_filter = lambda row: True
        self._select_keys = None
        self._order_key = None
        self._order_ascending = True
        self._limit = None

    @update_maps
    def from_(self, table):
        self.append_table(table)
        return self

    def join(self, table, *, on):
        self._join(table)
        self._on(on)
        return self

    @update_maps
    def _join(self, table):
        self.append_table(table)

    def _on(self, join_keys):
        key1, key2 = self._key_mapper.map(*join_keys)
        self._on_filter = lambda row: row[key1.table][key1.column] == row[key2.table][key2.column]

    def where(self, column, *, condition):
        [key] = self._key_mapper.map(column)
        self._where_filter = lambda row: condition(converted(row[key.table][key.column]))
        return self

    def select(self, columns):
        self._select_keys = self._key_mapper.map(*columns)
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
        rows = []
        for table in self._tables:
            with open(table.path) as table_file:
                _, current_rows = self._parse_table(table_file)
            rows.append(current_rows)
        return itertools.product(*rows)

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


class KeyMapper:
    def __init__(self, query):
        self.query: Select = query

    def map(self, *keys):
        return tuple(self._map(key) for key in keys)

    def _map(self, key):
        key_parts = key.split('.', maxsplit=1)
        if len(key_parts) == 1:
            return self._map_one_part(key)
        else:  # len == 2
            return self._map_two_parts(key_parts)

    def _map_one_part(self, key):
        matches = tuple((i, headers[key]) for i, headers in enumerate(self.query.header_maps) if key in headers)
        if len(matches) > 1:
            raise AmbiguousColumnNameError(key)
        if not matches:
            raise NoSuchColumnError(key)
        return Key(*matches[0])

    def _map_two_parts(self, parts):
        try:
            table_index = self.query.table_map[parts[0]]
            return Key(table=table_index, column=self.query.header_maps[table_index][parts[1]])
        except KeyError:
            raise NoSuchColumnError('.'.join(parts))


Key = collections.namedtuple('Key', ['table', 'column'])
