from .filtered_query import FilteredQuery


class DeleteQuery(FilteredQuery):
    def __init__(self):
        super().__init__()

    def run(self):
        pass
