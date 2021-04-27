from my_sqlite.query.delete_query import DeleteQuery


if __name__ == '__main__':
    DeleteQuery().from_('Players').where('nameFirst', lambda value: False).run()

