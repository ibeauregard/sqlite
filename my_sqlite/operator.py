import functools
import operator


def typesafe(func):
    @functools.wraps(func)
    def typesafe_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError:
            return False
    return typesafe_func


(operator.lt, operator.le, operator.eq, operator.ne, operator.ge, operator.gt)\
    = map(typesafe, (operator.lt, operator.le, operator.eq, operator.ne, operator.ge, operator.gt))


class Operator:
    mapping = {'<': operator.lt,
               '<=': operator.le,
               '=': operator.eq,
               '!=': operator.ne,
               '>=': operator.ge,
               '>': operator.gt}

    @classmethod
    def from_symbol(cls, symbol):
        return cls.mapping[symbol]
