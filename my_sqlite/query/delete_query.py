from .filtered_query import FilteredQuery
import os


class DeleteQuery(FilteredQuery):
    def __init__(self):
        super().__init__()

    def from_(self, table):
        self.main_table = table
        return self

    def run(self):
        with open(self.main_table) as table:
            column_headers, entries = self._parse_table(table)
            non_deleted_entries = [entry.values() for entry in entries if not self._filter(entry)]
        with open(self.main_table, 'w') as table:
            table.write(f"{self._sep.join(column_headers)}{self._linesep}"
                        f"{os.linesep.join(self._sep.join(entry) for entry in non_deleted_entries)}{self._linesep}")
