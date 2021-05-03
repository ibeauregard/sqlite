import sys

from my_sqlite.runner import Runner

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
            for query in filter(None, map(str.strip, ' '.join(lines).split(';'))):
                Runner.execute(query)
    except EOFError:
        sys.exit()
