from .abstract import AbstractQuery
from ..errors import NoSuchColumnError, AmbiguousColumnNameError, check_existence
from itertools import product


class Select(AbstractQuery):
    def __init__(self, result_columns):
        super().__init__()
        self._result_columns = result_columns
        self._right_table = None
        self._right_table_path = None
        self._input_join_keys = None
        self._order_key = None
        self._order_ascending = None
        self._limit = None

    def from_(self, table):
        self.table = table
        return self

    def join(self, right_table, *join_keys):
        self.right_table = right_table
        self._input_join_keys = join_keys
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
            cross_join = ({self._table: left_entry, self._right_table: right_entry}
                          for left_entry, right_entry in product(left_entries, right_entries))
            left_key, right_key = JoinKeyMatcher(self, left_headers, right_headers).run()
            2+2
            joined_table = (entry for entry in cross_join
                            if entry[left_key.table][left_key.column] == entry[right_key.table][right_key.column])


class JoinKeyMatcher:
    def __init__(self, query, left_headers, right_headers):
        self.query: Select = query
        self.left_headers = left_headers
        self.right_headers = right_headers

    def run(self):
        left_key = self._match(self.query._input_join_keys[0])
        right_key = self._match(self.query._input_join_keys[1])
        return left_key, right_key

    def _match(self, key):
        key_parts = key.split('.', maxsplit=1)
        if len(key_parts) == 1:
            return self._match_one_part(key)
        else:  # len == 2
            return Key(*key_parts)

    def _match_one_part(self, key):
        if key in self.left_headers and key in self.right_headers:
            raise AmbiguousColumnNameError(key)
        if key not in self.left_headers and key not in self.right_headers:
            raise NoSuchColumnError(key)
        table = self.query._table if key in self.left_headers else self.query.right_table
        return Key(table=table, column=key)


class Key:
    def __init__(self, table=None, column=None):
        self._table = table
        self._column = column

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, table):
        self._table = table

    @property
    def column(self):
        return self._column

    @column.setter
    def column(self, column):
        self._column = column
