import functools
import os
import shutil
from timeit import default_timer

from my_sqlite.error import InsertError
from my_sqlite.query import Delete, Insert, Select, Update
from my_sqlite.operator import operator
from my_sqlite.runner import error_handling


def display_time(test):
    @functools.wraps(test)
    def timed_test(*args, **kwargs):
        t = default_timer()
        return_value = test(*args, **kwargs)
        print(f'Test run in {default_timer() - t}s', end='\n\n')
        return return_value
    return timed_test


def run_test_suite():
    # Select

    @display_time
    @error_handling
    def select(columns, *, from_, join=None, where=None, order_by=None, limit=None):
        query = Select()
        query.from_(from_)
        if join is not None:
            query.join(join[0], on=join[1])
        if where is not None:
            query.where(where[0], condition=lambda value: where[1][0](value, where[1][1]))
        if columns is not None:
            query.select(columns)
        if order_by is not None:
            query.order_by(order_by)
        if limit is not None:
            query.limit(limit)
        query.run()

    select(('nAmEfIrSt', 'nAmElAsT', 'yeariD', 'batting.hR'),
           from_='players',
           join=('batting', ('players.iD', 'pLaYeRiD')),
           where=('batting.hR', (operator.gt, 20)),
           order_by=(('batting.hR', False), ('nAmElAsT', True), ('nAmEfIrSt', True), ('yEaRiD', True)),
           limit=10)

    select(('nAmEfIrSt', 'nAmElAsT'),
           from_='players',
           limit=10)

    select(('*',),
           from_='players',
           limit=10)

    select(('players.*',),
           from_='players',
           limit=10)

    select(('batting.*',),
           from_='players',
           limit=10)

    select(('nAmEfIrSt', '*', 'nAmElAsT'),
           from_='players',
           join=('batting', ('players.iD', 'pLaYeRiD')),
           where=('hR', (operator.gt, 20)),
           order_by=(('hR', False),),
           limit=10)

    select(('nAmEfIrSt', 'players.*', 'nAmElAsT'),
           from_='players',
           join=('batting', ('players.iD', 'pLaYeRiD')),
           where=('hR', (operator.gt, 20)),
           order_by=(('hR', False),),
           limit=10)

    select(('nAmEfIrSt', 'batting.*', 'nAmElAsT'),
           from_='players',
           join=('batting', ('players.iD', 'pLaYeRiD')),
           where=('hR', (operator.gt, 20)),
           order_by=(('hR', False),),
           limit=10)

    select(('nAmEfIrSt', 'pitching.*', 'nAmElAsT'),
           from_='players',
           join=('batting', ('players.iD', 'pLaYeRiD')),
           where=('hR', (operator.gt, 20)),
           order_by=(('hR', False),),
           limit=10)

    select(('roger', 'nAmElAsT'),
           from_='players',
           limit=10)

    select(('nAmEfIrSt', 'nAmElAsT'),
           from_='Roger',
           limit=10)

    select(('nAmEfIrSt', 'nAmElAsT', 'yeariD', 'hR'),
           from_='players',
           join=('batting', ('players.iD', 'pLaYeRiD')),
           where=('hR', (operator.gt, 20)),
           order_by=(('iD', False),),
           limit=10)

    # Update

    @display_time
    @error_handling
    def update(table, *, set, where=None):
        query = Update(table)
        query.set(set)
        if where is not None:
            query.where(where[0], condition=lambda value: where[1][0](value, where[1][1]))
        query.run()

    update('Roger', set={}, where=('roger', (operator.eq, True)))
    update('players', set={}, where=('roger', (operator.eq, True)))
    update('players', set={'roger': 'cyr'}, where=None)
    update('players', set={'bIrThCoUnTrY': 'USofA'}, where=('bIrThCoUnTrY', (operator.eq, 'USA')))
    update('players', set={'bIrThCoUnTrY': 'USA'}, where=('bIrThCoUnTrY', (operator.eq, 'USofA')))

    # Delete

    @display_time
    @error_handling
    def delete(*, from_, where=None):
        query = Delete()
        query.from_(from_)
        if where is not None:
            query.where(where[0], condition=lambda value: where[1][0](value, where[1][1]))
        query.run()

    delete(from_='Roger')
    delete(from_='players', where=('roger', (1, 2)))
    delete(from_='players', where=('nAmEgIvEn', (operator.eq, 'Roger Cyr')))
    delete(from_='players', where=('bIrThCoUnTrY', (operator.eq, 'USA')))

    # Insert

    @display_time
    @error_handling
    def insert(*, into, values):
        query = Insert()
        query.into(**into)
        if values is not None:
            if len(set(map(len, values))) > 1:
                raise InsertError('all VALUES must have the same number of terms')
            if into['columns']:
                length_discrepancy = next((len(row) for row in values if len(row) != len(into['columns'])), None)
                if length_discrepancy:
                    raise InsertError(f"{length_discrepancy} values for {len(into['columns'])} columns")
            query.values(values)
            query.run()

    insert(into={'table': 'Roger', 'columns': None},
           values=None)

    insert(into={'table': 'players', 'columns': ('iD', 'roger')},
           values=None)

    insert(into={'table': 'players', 'columns': None},
           values=None)

    insert(into={'table': 'players', 'columns': ('iD', 'nAmEfIrSt', 'nAmElAsT')},
           values=None)

    insert(into={'table': 'players', 'columns': ('iD', 'nAmEfIrSt', 'nAmElAsT')},
           values=((1, 2, 3), (1, 2, 3), (1, 2)))

    insert(into={'table': 'players', 'columns': ('iD', 'nAmEfIrSt', 'nAmElAsT')},
           values=((1, 2),))

    insert(into={'table': 'players', 'columns': None},
           values=((1, 2),))

    insert(into={'table': 'players', 'columns': ('nAmEfIrSt', 'nAmElAsT')},
           values=(('Roger', 'Cyr'), ('Eric', 'Pickup')))

    insert(into={'table': 'players', 'columns': ('iD', 'nAmEfIrSt', 'nAmElAsT')},
           values=(('999', 'Roger', 'Cyr'), ('9999', 'Eric', 'Pickup')))


if __name__ == '__main__':
    shutil.copy2('mlb/players.csv', 'mlb/players.csv.backup')
    try:
        run_test_suite()
    finally:
        shutil.copy2('mlb/players.csv.backup', 'mlb/players.csv')
        os.remove('mlb/players.csv.backup')
