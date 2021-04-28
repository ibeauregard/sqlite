import sys


class Config:
    separator = ','
    try:
        database_path = sys.argv[1]
    except IndexError:
        sys.exit('my_sqlite: Missing argument: path to the database')
    table_filename_extension = '.csv'
