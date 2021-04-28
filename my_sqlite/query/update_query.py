from .filtered_query import FilteredQuery


class UpdateQuery(FilteredQuery):
    def __init__(self, table):
        super().__init__()
        self.main_table = table
        self._update_dict = None

    def set(self, update_dict):
        self._update_dict = update_dict
        return self

    def run(self):
        try:
            with open(self.main_table) as table:
                column_headers, entries = self._parse_table(table)
                self._validate_update_dict(column_headers)
                updated_entries = [self._update(entry) for entry in entries]
            with open(self.main_table, 'w') as table:
                table.write(self._join_entries(column_headers, updated_entries))
        except FileNotFoundError:
            print(f'Error: no such table: {self._main_table}')
        except KeyError as e:
            print(f'Error: no such column: {e}')

    def _validate_update_dict(self, headers):
        difference = set(self._update_dict).difference(headers)
        if difference:
            raise KeyError(difference.pop())

    def _update(self, entry):
        if self._filter(entry):
            entry = {**entry, **self._update_dict}
        return entry.values()
