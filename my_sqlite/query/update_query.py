from .filtered_query import FilteredQuery


class UpdateQuery(FilteredQuery):
    def __init__(self, table):
        super().__init__()
        self.main_table = table
        self._data = None

    def set(self, data):
        self._data = data

    def run(self):
        pass
