from my_sqlite.query.delete import Delete
from my_sqlite.query.update import Update
from my_sqlite.query.select import Select
from my_sqlite.errors import NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError
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


def error_handling(test):
    @wraps(test)
    def test_with_error_handling(*args, **kwargs):
        try:
            test(*args, **kwargs)
        except NoSuchTableError as e:
            print(e)
        except NoSuchColumnError as e:
            print(e)
        except AmbiguousColumnNameError as e:
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
    def select(columns, table, right_table, join_keys, where_args=None):
        select = Select(columns)\
            .from_(table)\
            .join(right_table)\
            .on(*join_keys)
        if where_args is not None:
            select = select.where(*where_args)
        result = select.run()
        print(*('|'.join(entry) for entry in result), sep='\n')
    select(('nameFirst', 'nameLast', 'yearId', 'HR'),
         'Roger',
          'Batting', ('Players.playerID', 'Batting.playerID'))

    select(('nameFirst', 'nameLast', 'yearId', 'HR'),
         'Players',
          'Roger', ('Players.Players.playerID', 'Batting.playerID'))

    select(('nameFirst', 'nameLast', 'yearId', 'HR'),
         'Players',
          'Batting', ('Players.playerID', 'Batting.Batting.playerID'))

    select(('nameFirst', 'nameLast', 'yearId', 'HR'),
         'Players',
          'Batting', ('Batting.playerID', 'Players.playerID'), ('yearID', typesafe(lambda value: converted(value) == 2015)))

    select(('nameFirst', 'nameLast', 'yearId', 'HR'),
         'Players',
          'Batting', ('playerID', 'playerID'))

    select(('nameFirst', 'nameLast', 'yearId', 'HR'),
           'Players',
           'Batting', ('roger', 'playerID'))
