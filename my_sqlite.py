import sys

from config.config import Config
from my_sqlite.conversion import decoded, queries_from_input_lines
from my_sqlite.runner import QueryRunner

if __name__ == '__main__':
    print('\nmy_sqlite: DESCRIBE | SELECT | INSERT | UPDATE | DELETE')
    print('To exit the application, use CTRL + D.\n')
    try:
        while True:
            print(f'[{Config.database_path}]')
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
