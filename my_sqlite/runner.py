import collections
import functools
import re
from abc import ABC, abstractmethod

from my_sqlite.conversion import converted
from my_sqlite.error import NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError, InsertError, \
    QuerySyntaxError
from my_sqlite.operator import Operator
from my_sqlite.query import Select, Update, Delete, Insert


def non_null_argument(func):
    @functools.wraps(func)
    def wrapper(object, arg, *args, **kwargs):
        if arg is None:
            return
        return func(object, arg, *args, **kwargs)

    return wrapper


class AbstractSpecializedQueryRunner(ABC):
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

    def _from(self, raw_value):
        try:
            table = re.fullmatch(r'[\w.]+', raw_value).group()
        except AttributeError:
            raise QuerySyntaxError('FROM clause expects exactly one table name')
        self.query.from_(table)

    @non_null_argument
    def _where(self, raw_value):
        pattern = re.compile(r'([\w.]+)\s*(<=|<|=|!=|>=|>)\s*(?<=[^\\])"((?:\\"|[^"])*)(?<=[^\\])"\s*')
        try:
            column, operator, input_value = pattern.fullmatch(raw_value).groups()
        except AttributeError:
            raise QuerySyntaxError('WHERE clause syntax expected to be <column> <operator> "<value>",\n'
                                   '       where <operator> is one of <, <=, =, !=, >=, >')
        operator, input_value = Operator.from_symbol(operator), converted(input_value.replace(r'\"', '"'))
        self.query.where(column, condition=lambda value: operator(value, input_value))


class SelectQueryRunner(AbstractSpecializedQueryRunner):
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
        runner = cls()
        runner._from(from_)
        runner._join(join, on)
        runner._where(where)
        runner._select(select)
        runner._order_by(order_by)
        runner._limit(limit)
        print(*('|'.join(entry) for entry in runner.query.run()), sep='\n')

    @non_null_argument
    def _join(self, raw_join, raw_on):
        try:
            table = re.fullmatch(r'[\w.]+', raw_join).group()
        except AttributeError:
            raise QuerySyntaxError('JOIN clause expects exactly one table name')
        self.query.join(table, on=self._get_on_keys(raw_on))

    def _select(self, raw_value):
        self.query.select(filter(None, re.split(r'\s*,\s*', raw_value)))

    @non_null_argument
    def _order_by(self, raw_value):
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

    @non_null_argument
    def _limit(self, raw_value):
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


class UpdateQueryRunner(AbstractSpecializedQueryRunner):
    pattern = re.compile(
        r'(?i:UPDATE)\s+(?P<update>.+)\s+(?i:SET)\s+(?P<set_>[\s\S]+?)(\s+(?i:WHERE)\s+(?P<where>[\s\S]+?))?')

    def __init__(self):
        super().__init__()

    @classmethod
    def from_parts(cls, parts):
        update, set_, where = parts.groupdict().values()
        runner = cls()
        runner._update(update)
        runner._set(set_)
        runner._where(where)
        runner.query.run()

    def _update(self, raw_value):
        try:
            table = re.fullmatch(r'[\w.]+', raw_value).group()
        except AttributeError:
            raise QuerySyntaxError('UPDATE expects exactly one table name')
        self.query = Update(table)

    def _set(self, raw_value):
        # Matches one 'column = "value"' pair
        single_pair = re.compile(r'\s*(\w+)\s*=\s*(?<=[^\\])"((?:\\"|[^"])*)(?<=[^\\])"\s*')
        full_set = re.compile(single_pair.pattern + '(,' + single_pair.pattern + ')*')
        if not full_set.fullmatch(raw_value):
            raise QuerySyntaxError('wrong syntax in SET clause')
        update_dict = {column: value.replace(r'\"', '"') for column, value in single_pair.findall(raw_value)}
        self.query.set(update_dict)


class DeleteQueryRunner(AbstractSpecializedQueryRunner):
    pattern = re.compile(r'(?i:DELETE\s+FROM)\s+(?P<from_>.+?)(\s+(?i:WHERE)\s+(?P<where>[\s\S]+?))?')

    def __init__(self):
        super().__init__(query=Delete())

    @classmethod
    def from_parts(cls, parts):
        from_, where = parts.groupdict().values()
        runner = cls()
        runner._from(from_)
        runner._where(where)
        runner.query.run()


class InsertQueryRunner(AbstractSpecializedQueryRunner):
    pattern = re.compile(r'(?i:INSERT\s+INTO)\s+(?P<into>.+?)'
                         r'(\s+(\((?P<columns>.+)\)))?'
                         r'(\s+(?i:VALUES)\s+(?P<values>[\s\S]+))')
    QueryParts = collections.namedtuple('QueryParts', ['into', 'columns', 'values'])

    def __init__(self):
        super().__init__(query=Insert())
        self._columns = None

    @classmethod
    def from_parts(cls, parts):
        into, columns, values = parts.groupdict().values()
        runner = cls()
        runner._process_columns(columns)
        runner._into(into)
        runner._values(values)
        runner.query.run()

    def _into(self, raw_into):
        try:
            table = re.fullmatch(r'[\w.]+', raw_into).group()
        except AttributeError:
            raise QuerySyntaxError('UPDATE expects exactly one table name')
        self.query.into(table, columns=self._columns)

    @non_null_argument
    def _process_columns(self, raw_columns):
        self._columns = tuple(filter(None, re.split(r'\s*,\s*', raw_columns)))

    def _values(self, raw_content):
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


def error_handling(func):
    @functools.wraps(func)
    def func_with_error_handling(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError, InsertError, QuerySyntaxError) as e:
            print(e)

    return func_with_error_handling


class QueryRunner:
    runners = (SelectQueryRunner, UpdateQueryRunner, DeleteQueryRunner, InsertQueryRunner)

    @classmethod
    @error_handling
    def execute(cls, query_text):
        runners, parts = iter(cls.runners), None
        while parts is None:
            try:
                SpecializedQueryRunner = next(runners)
            except StopIteration:
                raise QuerySyntaxError('input matches no known query')
            parts = SpecializedQueryRunner.pattern.fullmatch(query_text)
        SpecializedQueryRunner.from_parts(parts)
