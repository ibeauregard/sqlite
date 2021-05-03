import collections
import functools
import re

from my_sqlite.conversion import converted
from my_sqlite.operator import Operator
from my_sqlite.query.select import Select


def non_null_argument(func):
    @functools.wraps(func)
    def wrapper(object, arg, *args, **kwargs):
        if arg is None:
            return
        return func(object, arg, *args, **kwargs)
    return wrapper


class SelectQueryRunner:
    pattern = r'(?i:SELECT)\s+(?P<select>.+)'\
              r'\s+(?i:FROM)\s+(?P<from_>.+?)'\
              r'(\s+(?i:JOIN)\s+(?P<join>.+?)(\s+(?i:ON)\s+(?P<on>.+?))?)?'\
              r'(\s+(?i:WHERE)\s+(?P<where>.+?))?'\
              r'(\s+(?i:ORDER\s+BY)\s+(?P<order_by>.+?))?'\
              r'(\s+(?i:LIMIT)\s+(?P<limit>.+?))?'
    not_word_neither_dot = r'[^.A-Za-z0-9_]+'

    def __init__(self):
        self.query = Select()

    @classmethod
    def from_parts(cls, parts):
        parts, runner = SelectQueryParts(**parts.groupdict()), cls()
        runner._from(parts.from_)
        runner._join(parts.join, parts.on)
        runner._where(parts.where)
        runner._select(parts.select)
        runner._order_by(parts.order_by)
        runner._limit(parts.limit)
        print(*('|'.join(entry) for entry in runner.query.run()), sep='\n')

    def _from(self, raw_value):
        try:
            [table] = re.split(self.not_word_neither_dot, raw_value)
        except ValueError:
            raise SyntaxError('Error: FROM clause expects exactly one table name: syntax error')
        self.query.from_(table)

    @non_null_argument
    def _join(self, raw_join, raw_on):
        try:
            [table] = re.split(self.not_word_neither_dot, raw_join)
        except ValueError:
            raise SyntaxError('Error: JOIN clause expects exactly one table name: syntax error')
        self.query.join(table, on=self._get_on_keys(raw_on))

    @non_null_argument
    def _where(self, raw_value):
        try:
            column, operator, input_value = (token.strip() for token in re.split(r'(<=|<|=|!=|>=|>)', raw_value))
            [column] = re.split(self.not_word_neither_dot, column)
        except ValueError:
            raise SyntaxError('Error: WHERE clause syntax expected to be <column> <operator> <value>, '
                              'where <operator> is one of <, <=, =, !=, >=, >: syntax error')
        operator, input_value = Operator.from_symbol(operator), converted(input_value)
        self.query.where(column, condition=lambda value: operator(value, input_value))

    def _select(self, raw_value):
        self.query.select(filter(None, re.split(r'\s*,\s*', raw_value)))

    @non_null_argument
    def _order_by(self, raw_value):
        raw_ordering_terms = re.split(r'\s*,\s*', raw_value)
        ordering_terms = []
        for raw_term in raw_ordering_terms:
            try:
                groupdict = re.fullmatch(r"(?P<column>[.A-Za-z0-9_]+)(\s+(?P<ascending>(?i:asc|desc)))?",
                                     raw_term).groupdict()
            except AttributeError:
                raise SyntaxError('Error: wrong syntax in ORDER BY clause: syntax error')
            column, ascending = groupdict.values()
            ordering_terms.append((column, ascending is None or ascending == 'asc'))
        self.query.order_by(ordering_terms)

    @non_null_argument
    def _limit(self, raw_value):
        try:
            self.query.limit(int(raw_value))
        except ValueError:
            raise SyntaxError('Error: LIMIT clause takes exactly one integer: syntax error')

    @non_null_argument
    def _get_on_keys(self, raw_value):
        try:
            [on_left], [on_right] = (re.split(self.not_word_neither_dot, key)
                                     for key in re.split(r'\s*=\s*', raw_value))
        except ValueError:
            raise SyntaxError('Error: ON clause syntax expected to be <column_1> = <column_2>: syntax error')
        return on_left, on_right


SelectQueryParts = collections.namedtuple('SelectQueryParts',
                                          ['select', 'from_', 'join', 'on', 'where', 'order_by', 'limit'])
