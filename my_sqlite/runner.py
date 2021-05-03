import functools
import re

from my_sqlite.errors import NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError, BulkInsertError
from my_sqlite.runners.select import SelectQueryRunner


def error_handling(func):
    @functools.wraps(func)
    def func_with_error_handling(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (NoSuchTableError, NoSuchColumnError, AmbiguousColumnNameError, BulkInsertError, SyntaxError) as e:
            print(e)
    return func_with_error_handling


class Runner:
    runners = (SelectQueryRunner,)

    @classmethod
    @error_handling
    def execute(cls, query_text):
        runners, parts = iter(cls.runners), None
        while parts is None:
            try:
                QueryRunner = next(runners)
            except StopIteration:
                raise SyntaxError('Error: input syntax matches no known query')
            parts = re.fullmatch(QueryRunner.pattern, query_text)
        QueryRunner.from_parts(parts)
