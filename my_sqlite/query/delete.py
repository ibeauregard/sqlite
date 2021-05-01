from .filtered import FilteredQuery
from ..errors import translate_key_error


class Delete(FilteredQuery):
    def __init__(self):
        super().__init__()

    def from_(self, table):
        self.append_table(table)
        return self

    @translate_key_error
    def run(self):
        table_path = self._tables[0].path
        with open(table_path) as table:
            header_map, entries = self._parse_table(table)
            non_deleted_entries = [entry for entry in entries if not self._where_filter((entry,))]
        with open(table_path, 'w') as table:
            table.write(self._join_entries(header_map, non_deleted_entries))
