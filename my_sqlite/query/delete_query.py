from .filtered_query import FilteredQuery
import os


class DeleteQuery(FilteredQuery):
    def __init__(self):
        super().__init__()

    def from_(self, table_name):
        self._main_table = f'{self.database_path}/{table_name}{self.file_extension}'
        return self

    def run(self):
        with open(self._main_table) as table:
            column_headers = self.strip_and_split(next(table))
            entries = (dict(zip(column_headers, entry)) for entry in map(self.strip_and_split, table))
            non_deleted_entries = [entry.values() for entry in entries if not self._filter(entry)]
        with open(self._main_table, 'w') as table:
            table.write(f"{','.join(column_headers)}\n"
                        f"{os.linesep.join(','.join(entry) for entry in non_deleted_entries)}\n")

    @staticmethod
    def strip_and_split(line):
        return line.strip().split(',')
