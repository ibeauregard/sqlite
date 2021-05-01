from .abstract import AbstractQuery
from ..errors import translate_key_error


class Insert(AbstractQuery):
    def __init__(self):
        super().__init__()
        self._indices = None

    def into(self, table, *, columns=None):
        self.append_table(table)
        self._translate_columns(columns)
        return self

    @translate_key_error
    def _translate_columns(self, columns):
        table_header = next(iter(self.table_map.values())).header_map
        self._indices = tuple(range(len(table_header))) if columns is None\
            else {table_header[column]: i for i, column in enumerate(columns)}

    def values(self, rows):
        return self

    def run(self):
        pass
