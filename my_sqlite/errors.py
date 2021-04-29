from functools import wraps
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


def check_existence(setter):
    @wraps(setter)
    def wrapper(query, table, *args, **kwargs):
        if not query.path_from_table_name(table).is_file():
            raise NoSuchTableError(table)
        setter(query, table, *args, **kwargs)
    return wrapper


def translate_key_error(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except KeyError as key:
            raise NoSuchColumnError(str(key).strip('"\''))
    return wrapper
