import sys


class Config:
    try:
        database_path = sys.argv[1]
    except IndexError:
        sys.exit('my_sqlite: Missing argument: path to the database')
    table_filename_extension = '.csv'

    # ASCII 31 (0x1F) Unit Separator - Used to indicate separation between units within a record.
    unit_separator = chr(31)

    # ASCII 30 (0x1E) Record Separator - Used to indicate separation between records within a table (within a group).
    record_separator = chr(30)
