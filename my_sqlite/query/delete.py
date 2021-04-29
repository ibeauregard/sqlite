from .abstract import AbstractQuery
from ..errors import translate_key_error


class Delete(AbstractQuery):
    def __init__(self):
        super().__init__()
        self._filter = lambda entry, header_map: True

    def from_(self, table):
        self.table = table
        return self

    # TODO: remove code duplication with update
    def where(self, column, condition):
        self._filter = lambda entry, header_map: condition(entry[header_map[column]])
        return self

    @translate_key_error
    def run(self):
        with open(self.table) as table:
            header_map, entries = self._parse_table(table)
            non_deleted_entries = [entry for entry in entries if not self._filter(entry, header_map)]
        with open(self.table, 'w') as table:
            table.write(self._join_entries(header_map, non_deleted_entries))
