from .abstract import AbstractQuery
from ..errors import NoSuchColumnError, translate_key_error


class Update(AbstractQuery):
    def __init__(self, table):
        super().__init__()
        self.table = table
        self._update_dict = None
        self._filter = lambda entry: True

    def set(self, update_dict):
        self._update_dict = update_dict
        return self

    def where(self, column, condition):
        self._filter = lambda entry, header_map: condition(entry[header_map[column]])
        return self

    @translate_key_error
    def run(self):
        with open(self.table) as table:
            header_map, entries = self._parse_table(table)
            self._validate_update_dict(header_map)
            updated_entries = [self._update(entry, header_map) for entry in entries]
        with open(self.table, 'w') as table:
            table.write(self._join_entries(header_map, updated_entries))

    def _validate_update_dict(self, headers):
        difference = set(self._update_dict).difference(headers)
        if difference:
            raise NoSuchColumnError(difference.pop())

    def _update(self, entry, header_map):
        if self._filter(entry, header_map):
            for column, value in self._update_dict.items():
                entry[header_map[column]] = value
        return entry
