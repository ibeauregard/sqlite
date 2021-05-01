from .filtered import FilteredQuery
from ..errors import NoSuchColumnError, translate_key_error


class Update(FilteredQuery):
    def __init__(self, table):
        super().__init__()
        self.append_table(table)
        self._update_dict = None

    def set(self, update_dict):
        self._update_dict = update_dict
        return self

    @translate_key_error
    def run(self):
        table_path = self._tables[0].path
        with open(table_path) as table:
            header_map, entries = self._parse_table(table)
            self._validate_update_dict(header_map)
            updated_entries = [self._update(entry) for entry in entries]
        with open(table_path, 'w') as table:
            table.write(self._join_entries(header_map, updated_entries))

    def _validate_update_dict(self, headers):
        difference = set(self._update_dict).difference(headers)
        if difference:
            raise NoSuchColumnError(difference.pop())

    def _update(self, entry):
        if self._where_filter((entry,)):
            for column, value in self._update_dict.items():
                entry[self.header_maps[0][column]] = value
        return entry
