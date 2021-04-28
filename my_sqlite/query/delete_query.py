from .filtered_query import FilteredQuery


class DeleteQuery(FilteredQuery):
    def __init__(self):
        super().__init__()

    def from_(self, table):
        self.main_table = table
        return self

    def run(self):
        try:
            with open(self.main_table) as table:
                column_headers, entries = self._parse_table(table)
                non_deleted_entries = [entry.values() for entry in entries if not self._filter(entry)]
            with open(self.main_table, 'w') as table:
                table.write(self._join_entries(column_headers, non_deleted_entries))
        except FileNotFoundError:
            print(f'Error: no such table: {self._main_table}')
        except KeyError as e:
            print(f'Error: no such column: {e}')
