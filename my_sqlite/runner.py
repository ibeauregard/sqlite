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
    not_word_neither_dot = re.compile(r'[^.A-Za-z0-9_]+')

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
            [table] = self.not_word_neither_dot.split(raw_value)
        except ValueError:
            raise QuerySyntaxError('FROM clause expects exactly one table name')
        self.query.from_(table)

    @non_null_argument
    def _where(self, raw_value):
        try:
            column, operator, input_value = (token.strip() for token in re.split(r'(<=|<|=|!=|>=|>)', raw_value))
            [column] = self.not_word_neither_dot.split(column)
        except ValueError:
            raise QuerySyntaxError('WHERE clause syntax expected to be <column> <operator> <value>, '
                                   'where <operator> is one of <, <=, =, !=, >=, >')
        operator, input_value = Operator.from_symbol(operator), converted(input_value.strip('\'\"'))
        self.query.where(column, condition=lambda value: operator(value, input_value))


class SelectQueryRunner(AbstractSpecializedQueryRunner):
    pattern = re.compile(r'(?i:SELECT)\s+(?P<select>.+)'
                         r'\s+(?i:FROM)\s+(?P<from_>.+?)'
                         r'(\s+(?i:JOIN)\s+(?P<join>.+?)(\s+(?i:ON)\s+(?P<on>.+?))?)?'
                         r'(\s+(?i:WHERE)\s+(?P<where>.+?))?'
                         r'(\s+(?i:ORDER\s+BY)\s+(?P<order_by>.+?))?'
                         r'(\s+(?i:LIMIT)\s+(?P<limit>.+?))?')
    QueryParts = collections.namedtuple('QueryParts', ['select', 'from_', 'join', 'on', 'where', 'order_by', 'limit'])

    def __init__(self):
        super().__init__(query=Select())

    @classmethod
    def from_parts(cls, parts):
        parts, runner = cls.QueryParts(**parts.groupdict()), cls()
        runner._from(parts.from_)
        runner._join(parts.join, parts.on)
        runner._where(parts.where)
        runner._select(parts.select)
        runner._order_by(parts.order_by)
        runner._limit(parts.limit)
        print(*('|'.join(entry) for entry in runner.query.run()), sep='\n')

    @non_null_argument
    def _join(self, raw_join, raw_on):
        try:
            [table] = self.not_word_neither_dot.split(raw_join)
        except ValueError:
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
                group_dict = re.fullmatch(r"(?P<column>[.A-Za-z0-9_]+)(\s+(?P<ascending>(?i:asc|desc)))?",
                                          raw_term).groupdict()
            except AttributeError:
                raise QuerySyntaxError('wrong syntax in ORDER BY clause')
            column, ascending = group_dict.values()
            ordering_terms.append((column, ascending is None or ascending == 'asc'))
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
            [on_left], [on_right] = (self.not_word_neither_dot.split(key)
                                     for key in re.split(r'\s*=\s*', raw_value))
        except ValueError:
            raise QuerySyntaxError('ON clause syntax expected to be <column_1> = <column_2>')
        return on_left, on_right


class UpdateQueryRunner(AbstractSpecializedQueryRunner):
    pattern = re.compile(r'(?i:UPDATE)\s+(?P<update>.+)\s+(?i:SET)\s+(?P<set>.+?)(\s+(?i:WHERE)\s+(?P<where>.+?))?')
    QueryParts = collections.namedtuple('QueryParts', ['update', 'set', 'where'])

    def __init__(self):
        super().__init__()

    @classmethod
    def from_parts(cls, parts):
        parts, runner = cls.QueryParts(**parts.groupdict()), cls()
        runner._update(parts.update)
        runner._set(parts.set)
        runner._where(parts.where)
        runner.query.run()

    def _update(self, raw_value):
        try:
            [table] = self.not_word_neither_dot.split(raw_value)
        except ValueError:
            raise QuerySyntaxError('UPDATE expects exactly one table name')
        self.query = Update(table)

    def _set(self, raw_value):
        single_pair = re.compile(r'\s*(\w+)\s*=\s*"([^"]*)"\s*')
        full_set = re.compile(single_pair.pattern + '(,' + single_pair.pattern + ')*')
        if not full_set.fullmatch(raw_value):
            raise QuerySyntaxError('wrong syntax in SET clause')
        update_dict = dict(single_pair.findall(raw_value))
        self.query.set(update_dict)


class DeleteQueryRunner(AbstractSpecializedQueryRunner):
    pattern = re.compile(r'(?i:DELETE\s+FROM)\s+(?P<from_>.+?)(\s+(?i:WHERE)\s+(?P<where>.+?))?')
    QueryParts = collections.namedtuple('QueryParts', ['from_', 'where'])

    def __init__(self):
        super().__init__(query=Delete())

    @classmethod
    def from_parts(cls, parts):
        parts, runner = cls.QueryParts(**parts.groupdict()), cls()
        runner._from(parts.from_)
        runner._where(parts.where)
        runner.query.run()


class InsertQueryRunner(AbstractSpecializedQueryRunner):
    pattern = re.compile(r'(?i:INSERT\s+INTO)\s+(?P<into>.+?)'
                         r'(\s+(\((?P<columns>.+)\)))?'
                         r'(\s+(?i:VALUES)\s+(?P<values>.+))')
    QueryParts = collections.namedtuple('QueryParts', ['into', 'columns', 'values'])

    def __init__(self):
        super().__init__(query=Insert())
        self._columns = None

    @classmethod
    def from_parts(cls, parts):
        parts, runner = cls.QueryParts(**parts.groupdict()), cls()
        runner._process_columns(parts.columns)
        runner._into(parts.into)
        runner._values(parts.values)
        runner.query.run()

    def _into(self, raw_into):
        try:
            [table] = self.not_word_neither_dot.split(raw_into)
        except ValueError:
            raise QuerySyntaxError('UPDATE expects exactly one table name')
        self.query.into(table, columns=self._columns)

    @non_null_argument
    def _process_columns(self, raw_columns):
        self._columns = tuple(filter(None, re.split(r'\s*,\s*', raw_columns)))

    def _values(self, raw_content):
        single_set = re.compile(r'\s*"[^"]*"\s*(?:,\s*"[^"]*"\s*)*')
        full_set = re.compile(r'\s*\(' + single_set.pattern + r'\)\s*(?:,\s*\(' + single_set.pattern + r'\)\s*)*')
        if not full_set.fullmatch(raw_content):
            raise QuerySyntaxError('wrong syntax in VALUES clause')
        rows = [[value.strip('"') for value in re.findall(r'"[^"]*"', row)]
                for row in re.findall(r'(?<=\()' + single_set.pattern + r'(?=\))', raw_content)]
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
