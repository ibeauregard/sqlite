import sys

from my_sqlite.conversion import decoded, queries_from_input_lines
from my_sqlite.runner import QueryRunner

if __name__ == '__main__':
    try:
        while True:
            lines, line = [], ''
            while not line or set(line) == {';'}:
                print('my_sqlite>', end=' ')
                line = input().strip()
            lines.append(line)
            while not line or line[-1] != ';':
                print('      ...>', end=' ')
                line = input().strip()
                lines.append(line)
            for query in queries_from_input_lines(lines):
                QueryRunner.execute(decoded(query))
                print()
    except EOFError:
        sys.exit()
