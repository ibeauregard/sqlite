import sys


class Config:
    try:
        database_path = sys.argv[1]
    except IndexError:
        sys.exit('my_sqlite: Missing argument: path to the database')
    table_filename_extension = '.csv'

    # ASCII 31 (0x1F) Unit Separator - Used to indicate separation between units within a record.
    # See https://en.wikipedia.org/wiki/C0_and_C1_control_codes#Basic_ASCII_control_codes.
    unit_separator = chr(31)

    # ASCII 30 (0x1E) Record Separator - Used to indicate separation between records within a table (within a group).
    # See https://en.wikipedia.org/wiki/C0_and_C1_control_codes#Basic_ASCII_control_codes.
    record_separator = chr(30)
