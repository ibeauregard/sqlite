from .filtered import FilteredQuery


class Delete(FilteredQuery):
    def __init__(self):
        super().__init__()

    def from_(self, table):
        self.append_table(table)
        return self

    def run(self):
        table = next(iter(self.table_map.values()))
        with open(table.path) as table_file:
            entries = self._parse_table(table_file)
            non_deleted_entries = [entry for entry in entries if not self._where_filter((entry,))]
        with open(table.path, 'w') as table_file:
            table_file.write(self._serialize_table(non_deleted_entries))
