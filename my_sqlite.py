from my_sqlite.query.delete_query import DeleteQuery
from my_sqlite.query.update_query import UpdateQuery
from functools import wraps
import operator


def converted(value):
    for converter in (int, float, lambda x: x):
        try:
            return converter(value)
        except ValueError:
            pass


def typesafe(func):
    @wraps(func)
    def typesafe_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError:
            return False
    return typesafe_func


if __name__ == '__main__':
    # DeleteQuery
    print('DeleteQuery')
    input_value = converted(300)
    table = 'Roger'
    column = 'weight'

    @typesafe
    def condition(value):
        return operator.lt(converted(value), input_value)
    DeleteQuery().from_(table).where(column, condition).run()
    table = 'Players'
    column = 'pickup'
    DeleteQuery().from_(table).where(column, condition).run()
    column = 'weight'
    DeleteQuery().from_(table).where(column, condition).run()

    # UpdateQuery
    print('\nUpdateQuery')

    @typesafe
    def condition(value):
        return operator.gt(converted(value), input_value)
    data = {'birthCountry': 'USofA'}

    table = 'Roger'
    UpdateQuery(table).set(data).where(column, condition).run()
    table = 'Players'

    data = {'pickup': 'USofA'}
    UpdateQuery(table).set(data).where(column, condition).run()
    data = {'birthCountry': 'USofA'}

    column = 'pickup'
    UpdateQuery(table).set(data).where(column, condition).run()
    column = 'weight'

    UpdateQuery(table).set(data).where(column, condition).run()
    data = {'birthCountry': 'USA'}
    UpdateQuery(table).set(data).where(column, condition).run()
