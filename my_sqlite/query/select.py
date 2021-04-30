import collections
import functools
import heapq
import itertools

from .abstract import AbstractQuery
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
        self._key_matcher = KeyMatcher(self)
        self.table_map = {}
        self.header_maps = []
        self._right_table = None
        self._right_table_path = None
        self._on_filter = lambda entry: True
        self._where_filter = lambda entry: True
        self._select_keys = None
        self._order_key = None
        self._order_ascending = True
        self._limit = None

    @update_maps
    def from_(self, table):
        self.table = table
        return self

    @update_maps
    def join(self, right_table):
        self.right_table = right_table
        return self

    def on(self, *join_keys):
        key1, key2 = self._key_matcher.match(*join_keys)
        self._on_filter = lambda entry: entry[key1.table][key1.column] == entry[key2.table][key2.column]
        return self

    def where(self, column, condition):
        [key] = self._key_matcher.match(column)
        self._where_filter = lambda entry: condition(entry[key.table][key.column])
        return self

    def select(self, *columns):
        self._select_keys = self._key_matcher.match(*columns)
        return self

    def order_by(self, key, *, ascending=True):
        [self._order_key] = self._key_matcher.match(key)
        self._order_ascending = ascending
        return self

    def limit(self, limit):
        self._limit = limit if limit >= 0 else None
        return self

    @property
    def right_table(self):
        return self._right_table_path

    @right_table.setter
    @check_existence
    def right_table(self, table):
        self._right_table = table
        self._right_table_path = str(self.path_from_table_name(table))

    # TODO: handle single table queries
    def run(self):
        with open(self.table) as left_table, open(self.right_table) as right_table:
            left_headers, left_entries = self._parse_table(left_table)
            right_headers, right_entries = self._parse_table(right_table)
            filtered_join = (entry for entry in itertools.product(left_entries, right_entries)
                             if self._on_filter(entry) and self._where_filter(entry))
        return ((entry[table][column] for table, column in self._select_keys) if self._select_keys
                else itertools.chain(*entry)
                for entry in self._order_and_limit()(filtered_join))

    def _order_and_limit(self):
        if self._order_key is not None:
            def key(entry): return entry[self._order_key.table][self._order_key.column]
            if self._limit:
                return lambda entries: \
                    (heapq.nsmallest if self._order_ascending else heapq.nlargest)(self._limit, entries, key=key)
            else:
                return lambda entries: sorted(entries, key=key, reverse=not self._order_ascending)
        else:
            return lambda entries: itertools.islice(entries, self._limit)


class KeyMatcher:
    def __init__(self, query):
        self.query: Select = query

    def match(self, *keys):
        return tuple(self._match(key) for key in keys)

    def _match(self, key):
        key_parts = key.split('.', maxsplit=1)
        if len(key_parts) == 1:
            return self._match_one_part(key)
        else:  # len == 2
            return self._match_two_parts(key_parts)

    def _match_one_part(self, key):
        matches = tuple((i, headers[key]) for i, headers in enumerate(self.query.header_maps) if key in headers)
        if len(matches) > 1:
            raise AmbiguousColumnNameError(key)
        if not matches:
            raise NoSuchColumnError(key)
        return Key(*matches[0])

    def _match_two_parts(self, parts):
        try:
            table_index = self.query.table_map[parts[0]]
            return Key(table=table_index, column=self.query.header_maps[table_index][parts[1]])
        except KeyError:
            raise NoSuchColumnError('.'.join(parts))


Key = collections.namedtuple('Key', ['table', 'column'])
