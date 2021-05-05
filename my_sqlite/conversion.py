import codecs
import re


def converted(value):
    for converter in (int, float, lambda x: x):
        try:
            return converter(value)
        except ValueError:
            pass


# See https://stackoverflow.com/a/37059682/2237433
# Leaving \" encoded
def decoded(string):
    return codecs.escape_decode(bytes(string.replace(r'\"', r'\\"'), "utf-8"))[0].decode("utf-8")


def queries_from_input_lines(lines):
    colon_outside_quotes = re.compile(
        r';(?=(?:(?:[^"]|\\")*(?<=[^\\])"(?:[^"]|\\")*(?<=[^\\])")*(?:[^"]|\\")*$)')
    return filter(None, map(str.strip, colon_outside_quotes.split(' '.join(lines))))
