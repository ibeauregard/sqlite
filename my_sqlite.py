from my_sqlite.query.delete_query import DeleteQuery
import operator


def converted(value):
    for converter in (int, float, lambda x: x):
        try:
            return converter(value)
        except ValueError:
            pass


def typesafe(func):
    def typesafe_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError:
            return False
    return typesafe_func


if __name__ == '__main__':
    input_value = converted(300)
    comparator = operator.lt
    table = 'Players'
    column = 'weight'

    @typesafe
    def condition(value):
        return comparator(converted(value), input_value)
    DeleteQuery().from_(table).where(column, condition).run()
