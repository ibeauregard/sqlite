import functools
from timeit import default_timer

from my_sqlite.errors import NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError
from my_sqlite.query.select import Select
from my_sqlite.operator import operator


def error_handling(test):
    @functools.wraps(test)
    def test_with_error_handling(*args, **kwargs):
        try:
            test(*args, **kwargs)
        except (NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError) as e:
            print(e)
    return test_with_error_handling


if __name__ == '__main__':
    # # Delete
    # print('DeleteQuery')
    # input_value = converted(300)
    # table = 'Roger'
    # column = 'weight'
    #
    # def condition(value):
    #     return operator.lt(converted(value), input_value)
    #
    # @error_handling
    # def delete(table, column, condition):
    #     Delete().from_(table).where(column, condition).run()
    # delete(table, column, condition)
    #
    # table = 'Players'
    # column = 'pickup'
    # delete(table, column, condition)
    # column = 'weight'
    # delete(table, column, condition)
    #
    # # Update
    # print('\nUpdateQuery')
    #
    # def condition(value):
    #     return operator.gt(converted(value), input_value)
    # data = {'birthCountry': 'USofA'}
    #
    # table = 'Roger'
    # @error_handling
    # def update(table, data, column, condition):
    #     Update(table).set(data).where(column, condition).run()
    # update(table, data, column, condition)
    # table = 'Players'
    #
    # data = {'pickup': 'USofA'}
    # update(table, data, column, condition)
    # data = {'birthCountry': 'USofA'}
    #
    # column = 'pickup'
    # update(table, data, column, condition)
    # column = 'weight'
    #
    # update(table, data, column, condition)
    # data = {'birthCountry': 'USA'}
    # update(table, data, column, condition)

    # Select
    print('\nSelectQuery')

    @error_handling
    def select(columns, *, from_, join=None, where=None, order_by=None, limit=None):
        query = Select().from_(from_)
        if join is not None:
            query = query.join(join[0], on=join[1])
        if where is not None:
            query = query.where(where[0], condition=lambda value: where[1][0](value, where[1][1]))
        if columns is not None:
            query = query.select(columns)
        if order_by is not None:
            query = query.order_by(order_by[0], ascending=order_by[1])
        if limit is not None:
            query = query.limit(limit)
        print(*('|'.join(entry) for entry in query.run()), sep='\n')

    t = default_timer()
    select(('nameFirst', 'nameLast', 'yearID', 'HR'),
           from_='Players',
           join=('Batting', ('Players.ID', 'playerID')),
           where=('HR', (operator.gt, 20)),
           order_by=('HR', False),
           limit=None)
    print(default_timer() - t)
