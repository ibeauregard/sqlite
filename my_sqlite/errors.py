from abc import ABC, abstractmethod


class NoSuchTableError(Exception):
    def __init__(self, table):
        super().__init__(table)
        self.table = table

    def __str__(self):
        return f'Error: no such table: {self.table}'


class ColumnError(ABC):
    def __init__(self, column):
        self.column = column

    @abstractmethod
    def __str__(self):
        pass


class NoSuchColumnError(ColumnError, Exception):
    def __str__(self):
        return f'Error: no such column: {self.column}'


class AmbiguousColumnNameError(ColumnError, Exception):
    def __str__(self):
        return f'Error: ambiguous column name: {self.column}'
