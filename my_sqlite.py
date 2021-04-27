from my_sqlite.query.delete_query import DeleteQuery
import operator


def converted(value):
    for converter in (int, float, lambda x: x):
        try:
            return converter(value)
        except ValueError:
            pass


def typesafe(comparison):
    def typesafe_comparison(value):
        try:
            return comparison(value)
        except TypeError:
            return False
    return typesafe_comparison


if __name__ == '__main__':
    input_value = converted(300)
    comparator = operator.lt
    table = 'Players'
    column = 'weight'
    DeleteQuery().from_(table).where(column, typesafe(lambda value: comparator(converted(value), input_value))).run()
