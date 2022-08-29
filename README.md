# my_sqlite

You can read my [Substack post that I wrote about this project](https://ianbeauregard.substack.com/p/emulating-sqlite-with-vanilla-python).

## Description

This project is a partial replica of the SQLite command-line interface (CLI) and data manipulation operations.

It is implemented with Python 3.6, using only modules of the [Python Standard Library](https://docs.python.org/3.6/library/index.html). Note that the data manipulation part of this project would arguably become trivial if one were to use a data manipulation tool such as the [pandas library](https://pandas.pydata.org/).

## Running my_sqlite

To run my_sqlite, you need [Python, version 3.6 or above](https://www.python.org/downloads/).

Launch the application by running `python my_sqlite.py <path-to-database>` from the project's root directory.

A database is already provided for demonstration purposes. It is located in the `mlb/` directory. It consists of a compilation of historical data about Major League Baseball. It is actually an excerpt from a much larger database which [can be found on Kaggle](https://www.kaggle.com/open-source-sports/baseball-databank).

## Database and table format
In this project, a database is represented as a directory containing multiples files using a uniform data storage format and a consistent naming scheme.

In each table, the first record is the header, which defines the name of each column. The rest of the records are the actual data. Each record's length matches the length of the header. Throughout all databases, records and columns are each consistently separated by a distinct, unique character.

These characters are specified in the config/config.py file. In addition to the separator characters, this file also specifies the common file extension for all tables, and the database path. Note that by default, the database path is the second argument of the `python` command used to launch my_sqlite.

## Types and conversion
The application does not maintain and enforce types, such as INT, FLOAT and TEXT. Instead, all data are stored as text. However, a conversion is performed on all user input, and on stored data whenever a value is needed for comparison or sorting.

First, the application tries to convert the value to an integer. It that conversion fails, a conversion to float is attempted. If that also fails, the value is considered as text.

Thanks to these conversions, the natural ordering of numerical types can be observed, i.e. a comparison such a 2 < 10 is true, whereas '2' < '10' is false.

## Unique ID constraint
By convention, a constraint of uniqueness is enforced on the values of the 0-th column of every table. 

## Escaped characters
You can use escape sequences in your queries, e.g. `\\`, `\'`, `\"`, `\n`, `\r`, `\t`, etc. This is particularly useful if you need to include quotes inside a `value` element (see the syntax diagrams below), or any control character such as a tab or a line feed. These characters will be decoded appropriately.

## The DESCRIBE statement

![Syntax of the DESCRIBE statement](diagrams/syntax/describe.svg?raw=true&sanitize=true)

## The SELECT statement

![Syntax of the SELECT statement](diagrams/syntax/select.svg?raw=true&sanitize=true)

## The INSERT statement

![Syntax of the INSERT statement](diagrams/syntax/insert.svg?raw=true&sanitize=true)

## The UPDATE statement

![Syntax of the UPDATE statement](diagrams/syntax/update.svg?raw=true&sanitize=true)

## The DELETE statement

![Syntax of the DELETE statement](diagrams/syntax/delete.svg?raw=true&sanitize=true)

## Running the tests

You can test the functionality of the my_sqlite.query module by running `python -m test.query <path-to-database>`.

You can test the functionality of the my_sqlite.builder module by running `python -m test.builder <path-to-database>`.

## Class diagram

![Class diagram](diagrams/class.png?raw=true)
