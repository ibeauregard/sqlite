import functools
from timeit import default_timer

from my_sqlite.errors import NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError
from my_sqlite.query.delete import Delete
from my_sqlite.query.insert import Insert
from my_sqlite.query.select import Select
from my_sqlite.operator import operator
from my_sqlite.query.update import Update


def display_time(test):
    @functools.wraps(test)
    def timed_test(*args, **kwargs):
        t = default_timer()
        return_value = test(*args, **kwargs)
        print(default_timer() - t, end='\n\n')
        return return_value
    return timed_test


def error_handling(test):
    @functools.wraps(test)
    def test_with_error_handling(*args, **kwargs):
        try:
            return test(*args, **kwargs)
        except (NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError) as e:
            print(e)
    return test_with_error_handling


if __name__ == '__main__':
    # # Select
    #
    # @display_time
    # @error_handling
    # def select(columns, *, from_, join=None, where=None, order_by=None, limit=None):
    #     query = Select().from_(from_)
    #     if join is not None:
    #         query = query.join(join[0], on=join[1])
    #     if where is not None:
    #         query = query.where(where[0], condition=lambda value: where[1][0](value, where[1][1]))
    #     if columns is not None:
    #         query = query.select(columns)
    #     if order_by is not None:
    #         query = query.order_by(order_by[0], ascending=order_by[1])
    #     if limit is not None:
    #         query = query.limit(limit)
    #     print(*('|'.join(entry) for entry in query.run()), sep='\n')
    #
    # select(('nameFirst', 'nameLast', 'yearID', 'HR'),
    #        from_='Players',
    #        join=('Batting', ('Players.ID', 'playerID')),
    #        where=('HR', (operator.gt, 20)),
    #        order_by=('HR', False),
    #        limit=10)
    #
    # select(('nameFirst', 'nameLast'),
    #        from_='Players',
    #        # join=('Batting', ('Players.ID', 'playerID')),
    #        # where=('HR', (operator.gt, 20)),
    #        # order_by=('HR', False),
    #        limit=10)
    #
    # select(('roger', 'nameLast'),
    #        from_='Players',
    #        # join=('Batting', ('Players.ID', 'playerID')),
    #        # where=('HR', (operator.gt, 20)),
    #        # order_by=('HR', False),
    #        limit=10)
    #
    # select(('nameFirst', 'nameLast'),
    #        from_='Roger',
    #        # join=('Batting', ('Players.ID', 'playerID')),
    #        # where=('HR', (operator.gt, 20)),
    #        # order_by=('HR', False),
    #        limit=10)
    #
    # select(('nameFirst', 'nameLast', 'yearID', 'HR'),
    #        from_='Players',
    #        join=('Batting', ('Players.ID', 'playerID')),
    #        where=('HR', (operator.gt, 20)),
    #        order_by=('ID', False),
    #        limit=10)
    #
    # # Update
    #
    # @display_time
    # @error_handling
    # def update(table, *, set, where=None):
    #     query = Update(table).set(set)
    #     if where is not None:
    #         query = query.where(where[0], condition=lambda value: where[1][0](value, where[1][1]))
    #     query.run()
    #
    # update('Roger', set={}, where=('roger', (operator.eq, True)))
    # update('Players', set={}, where=('roger', (operator.eq, True)))
    # update('Players', set={'roger': 'cyr'}, where=None)
    # update('Players', set={'birthCountry': 'USofA'}, where=('birthCountry', (operator.eq, 'USA')))
    # update('Players', set={'birthCountry': 'USA'}, where=('birthCountry', (operator.eq, 'USofA')))
    #
    # # Delete
    #
    # @display_time
    # @error_handling
    # def delete(*, from_, where=None):
    #     query = Delete().from_(from_)
    #     if where is not None:
    #         query = query.where(where[0], condition=lambda value: where[1][0](value, where[1][1]))
    #     query.run()
    #
    # delete(from_='Roger')
    # delete(from_='Players', where=('roger', (1, 2)))
    # delete(from_='Players', where=('nameGiven', (operator.eq, 'Roger Cyr')))
    # delete(from_='Players', where=('birthCountry', (operator.eq, 'USA')))

    # Insert

    @display_time
    @error_handling
    def insert(*, into, values):
        Insert().into(**into).values(values).run()

    insert(into={'table': 'Roger', 'columns': None},
           values=None)

    insert(into={'table': 'Players', 'columns': ('ID', 'roger')},
           values=None)
