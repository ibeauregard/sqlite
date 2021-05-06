import functools
import re
from abc import ABC, abstractmethod

from my_sqlite.conversion import converted
from my_sqlite.error import QuerySyntaxError, InsertError
from my_sqlite.operator import Operator
from my_sqlite.query import Select, Update, Delete, Insert, Describe


def return_self(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        method(self, *args, **kwargs)
        return self

    return wrapper


def non_null_argument(method):
    @functools.wraps(method)
    def wrapper(self, arg, *args, **kwargs):
        if arg is None:
            return None
        return method(self, arg, *args, **kwargs)

    return wrapper


class AbstractQueryBuilder(ABC):
    def __init__(self, *, query=None):
        self.query = query

    @staticmethod
    @property
    @abstractmethod
    def pattern():
        pass

    @classmethod
    @abstractmethod
    def from_parts(cls, parts):
        pass

    @return_self
    def from_(self, raw_value):
        try:
            table = re.fullmatch(r'[\w.]+', raw_value).group()
        except AttributeError:
            raise QuerySyntaxError('FROM clause expects exactly one table name')
        self.query.from_(table)

    @return_self
    @non_null_argument
    def where(self, raw_value):
        pattern = re.compile(r'([\w.]+)\s*(<=|<|=|!=|>=|>)\s*(?<=[^\\])"((?:\\"|[^"])*)(?<=[^\\])"\s*')
        try:
            column, operator, input_value = pattern.fullmatch(raw_value).groups()
        except AttributeError:
            raise QuerySyntaxError('WHERE clause syntax expected to be <column> <operator> "<value>",\n'
                                   '       where <operator> is one of <, <=, =, !=, >=, >')
        operator, input_value = Operator.from_symbol(operator), converted(input_value.replace(r'\"', '"'))
        self.query.where(column, condition=lambda value: operator(value, input_value))


class DescribeQueryBuilder(AbstractQueryBuilder):
    pattern = re.compile(r'(?i:DESCRIBE)\s+(?P<table>.+)')

    def __init__(self):
        super().__init__()

    @classmethod
    def from_parts(cls, parts):
        [table] = parts.groupdict().values()
        return cls().describe(table).query

    @return_self
    def describe(self, table):
        self.query = Describe(table)


class SelectQueryBuilder(AbstractQueryBuilder):
    pattern = re.compile(r'(?i:SELECT)\s+(?P<select>.+)'
                         r'\s+(?i:FROM)\s+(?P<from_>.+?)'
                         r'(\s+(?i:JOIN)\s+(?P<join>.+?)(\s+(?i:ON)\s+(?P<on>.+?))?)?'
                         r'(\s+(?i:WHERE)\s+(?P<where>[\s\S]+?))?'
                         r'(\s+(?i:ORDER\s+BY)\s+(?P<order_by>.+?))?'
                         r'(\s+(?i:LIMIT)\s+(?P<limit>.+?))?')

    def __init__(self):
        super().__init__(query=Select())

    @classmethod
    def from_parts(cls, parts):
        select, from_, join, on, where, order_by, limit = parts.groupdict().values()
        return (cls()
                .from_(from_)
                .join(join, on)
                .where(where)
                .select(select)
                .order_by(order_by)
                .limit(limit)
                .query)

    @return_self
    @non_null_argument
    def join(self, raw_join, raw_on):
        try:
            table = re.fullmatch(r'[\w.]+', raw_join).group()
        except AttributeError:
            raise QuerySyntaxError('JOIN clause expects exactly one table name')
        self.query.join(table, on=self._get_on_keys(raw_on))

    @return_self
    def select(self, raw_value):
        self.query.select(filter(None, re.split(r'\s*,\s*', raw_value)))

    @return_self
    @non_null_argument
    def order_by(self, raw_value):
        raw_ordering_terms = re.split(r'\s*,\s*', raw_value)
        ordering_terms = []
        for raw_term in raw_ordering_terms:
            try:
                column, ascending = re.fullmatch(r"(?P<column>[\w.]+)(\s+(?P<ascending>(?i:asc|desc)))?",
                                                 raw_term).groupdict().values()
            except AttributeError:
                raise QuerySyntaxError('wrong syntax in ORDER BY clause')
            ordering_terms.append((column, ascending is None or ascending.lower() == 'asc'))
        self.query.order_by(ordering_terms)

    @return_self
    @non_null_argument
    def limit(self, raw_value):
        try:
            self.query.limit(int(raw_value))
        except ValueError:
            raise QuerySyntaxError('LIMIT clause takes exactly one integer')

    @non_null_argument
    def _get_on_keys(self, raw_value):
        try:
            on_left, on_right = re.fullmatch(r'([\w.]+)\s*=\s*([\w.]+)\s*', raw_value).groups()
        except AttributeError:
            raise QuerySyntaxError('ON clause syntax expected to be <column_1> = <column_2>')
        return on_left, on_right


class UpdateQueryBuilder(AbstractQueryBuilder):
    pattern = re.compile(
        r'(?i:UPDATE)\s+(?P<update>.+)\s+(?i:SET)\s+(?P<set_>[\s\S]+?)(\s+(?i:WHERE)\s+(?P<where>[\s\S]+?))?')

    def __init__(self):
        super().__init__()

    @classmethod
    def from_parts(cls, parts):
        update, set_, where = parts.groupdict().values()
        return (cls()
                .update(update)
                .set_(set_)
                .where(where)
                .query)

    @return_self
    def update(self, raw_value):
        try:
            table = re.fullmatch(r'[\w.]+', raw_value).group()
        except AttributeError:
            raise QuerySyntaxError('UPDATE expects exactly one table name')
        self.query = Update(table)

    @return_self
    def set_(self, raw_value):
        # Matches one 'column = "value"' pair
        single_pair = re.compile(r'\s*(\w+)\s*=\s*(?<=[^\\])"((?:\\"|[^"])*)(?<=[^\\])"\s*')
        full_set = re.compile(single_pair.pattern + '(,' + single_pair.pattern + ')*')
        if not full_set.fullmatch(raw_value):
            raise QuerySyntaxError('wrong syntax in SET clause')
        update_dict = {column: value.replace(r'\"', '"') for column, value in single_pair.findall(raw_value)}
        self.query.set(update_dict)


class DeleteQueryBuilder(AbstractQueryBuilder):
    pattern = re.compile(r'(?i:DELETE\s+FROM)\s+(?P<from_>.+?)(\s+(?i:WHERE)\s+(?P<where>[\s\S]+?))?')

    def __init__(self):
        super().__init__(query=Delete())

    @classmethod
    def from_parts(cls, parts):
        from_, where = parts.groupdict().values()
        return (cls()
                .from_(from_)
                .where(where)
                .query)


class InsertQueryBuilder(AbstractQueryBuilder):
    pattern = re.compile(r'(?i:INSERT\s+INTO)\s+(?P<into>.+?)'
                         r'(\s+(\((?P<columns>.+)\)))?'
                         r'(\s+(?i:VALUES)\s+(?P<values>[\s\S]+))')

    def __init__(self):
        super().__init__(query=Insert())
        self._columns = None

    @classmethod
    def from_parts(cls, parts):
        into, columns, values = parts.groupdict().values()
        return (cls()
                .columns(columns)
                .into(into)
                .values(values)
                .query)

    @return_self
    def into(self, raw_into):
        try:
            table = re.fullmatch(r'[\w.]+', raw_into).group()
        except AttributeError:
            raise QuerySyntaxError('INSERT expects exactly one table name')
        self.query.into(table, columns=self._columns)

    @return_self
    @non_null_argument
    def columns(self, raw_columns):
        self._columns = tuple(filter(None, re.split(r'\s*,\s*', raw_columns)))

    @return_self
    def values(self, raw_content):
        # Matches one '("value1", "value2", ...)' set
        single_set = re.compile(
            r'\s*\(\s*(?<=[^\\])"(?:\\"|[^"])*(?<=[^\\])"\s*(?:,\s*(?<=[^\\])"(?:\\"|[^"])*(?<=[^\\])"\s*)*\)\s*')
        full_set = re.compile(single_set.pattern + '(?:,' + single_set.pattern + ')*')
        if not full_set.fullmatch(raw_content):
            raise QuerySyntaxError('wrong syntax in VALUES clause')
        rows = [[value.replace(r'\"', '"') for value in re.findall(r'(?<=[^\\])"((?:\\"|[^"])*)(?<=[^\\])"', row)]
                for row in single_set.findall(raw_content)]
        if len(set(map(len, rows))) > 1:
            raise InsertError('all VALUES must have the same number of terms')
        if self._columns:
            length_discrepancy = next((len(row) for row in rows if len(row) != len(self._columns)), None)
            if length_discrepancy:
                raise InsertError(f"{length_discrepancy} values for {len(self._columns)} columns")
        self.query.values(rows)
