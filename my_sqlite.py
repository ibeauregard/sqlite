import functools
import operator

from my_sqlite.errors import NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError
from my_sqlite.query.delete import Delete
from my_sqlite.query.select import Select
from my_sqlite.query.update import Update


def converted(value):
    for converter in (int, float, lambda x: x):
        try:
            return converter(value)
        except ValueError:
            pass


def typesafe(func):
    @functools.wraps(func)
    def typesafe_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError:
            return False
    return typesafe_func


def error_handling(test):
    @functools.wraps(test)
    def test_with_error_handling(*args, **kwargs):
        try:
            test(*args, **kwargs)
        except (NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError) as e:
            print(e)
    return test_with_error_handling


if __name__ == '__main__':
    # Delete
    print('DeleteQuery')
    input_value = converted(300)
    table = 'Roger'
    column = 'weight'

    @typesafe
    def condition(value):
        return operator.lt(converted(value), input_value)

    @error_handling
    def delete(table, column, condition):
        Delete().from_(table).where(column, condition).run()
    delete(table, column, condition)

    table = 'Players'
    column = 'pickup'
    delete(table, column, condition)
    column = 'weight'
    delete(table, column, condition)

    # Update
    print('\nUpdateQuery')

    @typesafe
    def condition(value):
        return operator.gt(converted(value), input_value)
    data = {'birthCountry': 'USofA'}

    table = 'Roger'
    @error_handling
    def update(table, data, column, condition):
        Update(table).set(data).where(column, condition).run()
    update(table, data, column, condition)
    table = 'Players'

    data = {'pickup': 'USofA'}
    update(table, data, column, condition)
    data = {'birthCountry': 'USofA'}

    column = 'pickup'
    update(table, data, column, condition)
    column = 'weight'

    update(table, data, column, condition)
    data = {'birthCountry': 'USA'}
    update(table, data, column, condition)

    # Select
    print('\nSelectQuery')

    @error_handling
    def select(table, right_table=None, join_keys=None, where_args=None, select_args=None, orderby_args=None, limit=None):
        query = Select()\
            .from_(table)
        if right_table:
            query = query.join(right_table).on(*join_keys)
        if where_args is not None:
            query = query.where(*where_args)
        if select_args is not None:
            query = query.select(*select_args)
        if orderby_args is not None:
            query = query.order_by(**orderby_args)
        if limit is not None:
            query = query.limit(limit)
        result = query.run()
        print(*('|'.join(entry) for entry in result), sep='\n')
    select('Roger',
          'Batting', ('Players.playerID', 'Batting.playerID'))

    select('Players',
          'Roger', ('Players.Players.playerID', 'Batting.playerID'))

    select('Players',
          'Batting', ('Players.playerID', 'Batting.Batting.playerID'))

    select('Players',
          'Batting', ('Batting.playerID', 'Players.playerID'),
           ('Batting.yearID', typesafe(lambda value: converted(value) == 2015)),
           ('nameFirst', 'nameLast', 'yearID', 'R', 'H', 'HR', 'RBI'),
           {'key': 'HR', 'ascending': False},
           10)

    select('Players',
           None, None, None, ('nameFirst', 'nameLast'),
           {'key': 'nameLast', 'ascending': False},
           10)

    select('Players',
          'Batting', ('playerID', 'playerID'))

    select('Players',
           'Batting', ('roger', 'playerID'))
