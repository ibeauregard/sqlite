import functools

from my_sqlite.builder import SelectQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder, InsertQueryBuilder, \
    DescribeQueryBuilder
from my_sqlite.error import NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError, InsertError, \
    QuerySyntaxError, UpdateError


def error_handling(func):
    @functools.wraps(func)
    def func_with_error_handling(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError, InsertError, UpdateError,
                QuerySyntaxError) as e:
            print(e)

    return func_with_error_handling


class QueryRunner:
    builders = (DescribeQueryBuilder, SelectQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder, InsertQueryBuilder)

    @classmethod
    @error_handling
    def execute(cls, query_text):
        builders, parts = iter(cls.builders), None
        while parts is None:
            try:
                QueryBuilder = next(builders)
            except StopIteration:
                raise QuerySyntaxError('input matches no known query')
            parts = QueryBuilder.pattern.fullmatch(query_text)
        query = QueryBuilder.from_parts(parts)
        query.run()
