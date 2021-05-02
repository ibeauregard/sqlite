from .filtered import FilteredQuery
from ..errors import translate_key_error


class Update(FilteredQuery):
    def __init__(self, table):
        super().__init__()
        self.append_table(table)
        self._update_dict = None

    @translate_key_error
    def set(self, update_dict):
        header_map = next(iter(self.table_map.values())).header_map
        self._update_dict = {header_map[col.lower()]: value for col, value in update_dict.items()}
        return self

    def run(self):
        table = next(iter(self.table_map.values()))
        with open(table.path) as table_file:
            entries = self._parse_table(table_file)
            updated_entries = (self._update(entry) for entry in entries)
        with open(table.path, 'w') as table_file:
            table_file.write(self._serialize_table(updated_entries))

    def _update(self, entry):
        if self._where_filter((entry,)):
            for column, value in self._update_dict.items():
                entry[column] = value
        return entry
