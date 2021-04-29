from collections import namedtuple
from .abstract import AbstractQuery
from ..errors import NoSuchColumnError, AmbiguousColumnNameError, check_existence
from itertools import product, chain
from functools import wraps


def update_maps(method):
    @wraps(method)
    def wrapper(query, table_name, *args, **kwargs):
        return_value = method(query, table_name, *args, **kwargs)
        with open(query.path_from_table_name(table_name)) as table:
            headers = query._strip_and_split(next(table))
        query.table_map[table_name] = max(query.table_map.values(), default=-1) + 1
        query.header_maps.append({header: i for i, header in enumerate(headers)})
        return return_value
    return wrapper


class Select(AbstractQuery):
    def __init__(self, result_columns):
        super().__init__()
        self.table_map = {}
        self.header_maps = []
        self._right_table = None
        self._right_table_path = None
        self._join_keys = None
        self._filter = lambda entry: True


        self._result_columns = result_columns
        self._order_key = None
        self._order_ascending = None
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
        self._join_keys = KeyMatcher(self).match(*join_keys)
        return self

    def where(self, column, condition):
        key = KeyMatcher(self).match(column)[0]
        self._filter = lambda entry: condition(entry[key.table][key.column])
        return self

    def order_by(self, key, ascending=True):
        self._order_key = key
        self._order_ascending = ascending
        return self

    def limit(self, limit):
        self._limit = limit
        return self

    @property
    def right_table(self):
        return self._right_table_path

    @right_table.setter
    @check_existence
    def right_table(self, table):
        self._right_table = table
        self._right_table_path = str(self.path_from_table_name(table))

    def run(self):
        with open(self.table) as left_table, open(self.right_table) as right_table:
            left_headers, left_entries = self._parse_table(left_table)
            right_headers, right_entries = self._parse_table(right_table)
            filtered_join = (entry for entry in product(left_entries, right_entries)
                             if entry[self._join_keys[0].table][self._join_keys[0].column]
                             == entry[self._join_keys[1].table][self._join_keys[1].column]
                             and self._filter(entry))
        return (list(chain(*entry)) for entry in filtered_join)


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
            return Key(table_index, self.query.header_maps[table_index][parts[1]])
        except KeyError:
            raise NoSuchColumnError('.'.join(parts))


Key = namedtuple('Key', ['table', 'column'])
