from .abstract import AbstractQuery
from ..errors import translate_key_error, BulkInsertError


class Insert(AbstractQuery):
    def __init__(self):
        super().__init__()
        self._value_indices = None
        self._inserted_rows = None

    def into(self, table, *, columns=None):
        self.append_table(table)
        self._translate_columns(columns)
        return self

    @translate_key_error
    def _translate_columns(self, columns):
        table_header = next(iter(self.table_map.values())).header_map
        if columns is None:
            self._value_indices = tuple(range(len(table_header)))
        else:
            self._value_indices = {table_header[column.lower()]: i for i, column in enumerate(columns)}
            if 0 not in self._value_indices:
                raise BulkInsertError(f"the value of the column at index 0 must be specified")

    def values(self, rows):
        row_len, num_columns = len(rows[0]), len(self._value_indices)
        if row_len != num_columns:
            raise BulkInsertError(f"table {next(iter(self.table_map))} has {num_columns} columns "
                                  f"but {row_len} values were supplied")
        self._inserted_rows = rows
        return self

    def run(self):
        table = next(iter(self.table_map.values()))
        with open(table.path) as table_file:
            rows = self._parse_table(table_file)
        existing_ids = set(row[0] for row in rows)
        row_len = len(table.header_map)
        rows_to_insert = []
        for row in self._inserted_rows:
            row_id = row[self._value_indices[0]]
            if row_id in existing_ids:
                raise BulkInsertError(f"a row already exists with id {row_id}; aborting the insert")
            rows_to_insert.append(
                [row[self._value_indices[i]] if i in self._value_indices else '' for i in range(row_len)])
        with open(table.path, 'a') as table_file:
            table_file.write(self._serialize_rows(rows_to_insert))
