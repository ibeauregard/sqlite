def converted(value):
    for converter in (int, float, lambda x: x):
        try:
            return converter(value)
        except ValueError:
            pass
