import os
import shutil

from my_sqlite.runner import QueryRunner


class TestSuite:
    @staticmethod
    def run(queries):
        for query, expected in queries:
            print(f"Running '{query}'")
            QueryRunner.execute(query)
            print(f"Expected: {expected}\n")


if __name__ == '__main__':
    select_queries = [('SELECT * FROM players JOIN batting ON a,b,c', 'Syntax error'),

                      ('SELECT * FROM players WHERE id=42', 'Syntax error'),

                      ('SELECT * FROM players WHERE id=42=24', 'Syntax error'),

                      ('SELECT * FROM players WHERE id', 'Syntax error'),

                      ('SELECT * FROM players JOIN batting, pitching', 'Syntax error'),

                      ('SELECT * FROM players ORDER BY id ???', 'Syntax error'),

                      ('SELECT * FROM players ORDER BY id nameLast', 'Syntax error'),

                      ('SELECT * FROM players LIMIT ten', 'Syntax error'),

                      ('SELECT FROM players', 'Syntax error'),

                      ('SELECT * FROM players LIMIT 10', 'Success'),

                      ('SELECT nameLast, nameFirst FROM players LIMIT 10', 'Success'),

                      ('SELECT players.nameLast, players.nameFirst FROM players LIMIT 10', 'Success'),

                      ('SELECT * FROM players WHERE id="0"', 'Success'),

                      ('SELECT nameLast, nameFirst, yearId, hr '
                       'FROM players JOIN batting ON batting.playerId = players.id '
                       'ORDER BY hr DESC, nameLast, nameFirst, yearId LIMIT 10', 'Success')]

    update_queries = [('UPDATE players', 'Syntax error'),

                      ('UPDATE players, batting SET id = "42"', 'Syntax error'),

                      ('UPDATE players SET a = b', 'Syntax error'),

                      ('UPDATE players SET id = 1000', 'Syntax error'),

                      ('UPDATE players SET id = "42", nameGiven: "Roger"', 'Syntax error'),

                      ('UPDATE players SET (birthCountry = "USofA", deathCountry = "Canada") '
                       'WHERE birthCountry = "USA"', 'Syntax error'),

                      ('UPDATE players SET id = "42000" WHERE id = "42"', 'Success'),

                      ('UPDATE players SET birthCountry = "USofA", deathCountry = "Canada" '
                       'WHERE birthCountry = "USA"', 'Success')]

    insert_queries = [('INSERT INTO players', 'Syntax error'),

                      ('INSERT INTO players, players (id) VALUES ("100")', 'Syntax error'),

                      ('INSERT INTO players (id, nameLast, nameFirst) VALUES a, b, c', 'Syntax error'),

                      ('INSERT INTO players (id, nameLast, nameFirst) VALUES (a, b, c), (d, e, f)', 'Syntax error'),

                      ('INSERT INTO players (id, nameLast, nameFirst) VALUES (500, Roger, Clemens)', 'Syntax error'),

                      ('INSERT INTO players (id, nameLast, nameFirst) '
                       'VALUES ("999", "Roger", "Clemens"), ("1000", "Roger")', 'Insert error'),

                      ('INSERT INTO players (id) VALUES ("999", "Roger"), ("1000", "Roger")', 'Insert error'),

                      ('INSERT INTO players (id) VALUES ("9991")', 'Success'),

                      ('INSERT INTO players (id) VALUES ("9992"), ("9993")', 'Success'),

                      ('INSERT INTO players (id, nameLast, nameFirst) '
                       'VALUES ("9994", "Roger", "Clemens")', 'Success'),

                      ('INSERT INTO players (id, nameLast, nameFirst) '
                       'VALUES ("9995", "Roger", "Clemens"), ("9996", "Jackie", "Robinson")', 'Success'),

                      ('INSERT INTO players VALUES ("9997", "", "", "", "", "", "", "", '
                       '"", "", "", "", "", "", "", '
                       '"", "", "", "", "", "", "")', 'Success'),

                      ('INSERT INTO players VALUES ("9998", "", "", "", "", "", "", "", '
                       r'"", "", "", "You can \"also\" insert quotes", "", "", "", '
                       '"", "", "", "", "", "", ""),'
                       ''
                       '("9999", "", "", "", "", "", "", "", '
                       '"", "", "", "and control characters, such as\n the line feed", "", "", "", '
                       '"", "", "", "", "", "", "")', 'Success')]

    delete_queries = [('DELETE players WHERE id=42', 'Syntax error'),

                      ('DELETE players', 'Syntax error'),

                      ('DELETE FROM players WHERE id=42', 'Syntax error'),

                      ('DELETE FROM players WHERE id=42=24', 'Syntax error'),

                      ('DELETE FROM players WHERE id', 'Syntax error'),

                      ('DELETE FROM players WHERE id = "919"', 'Success'),

                      ('DELETE FROM players', 'Success')]

    shutil.copy2('mlb/players.csv', 'mlb/players.csv.backup')
    try:
        TestSuite.run(select_queries)
        TestSuite.run(update_queries)
        TestSuite.run(insert_queries)
        TestSuite.run(delete_queries)
    finally:
        shutil.copy2('mlb/players.csv.backup', 'mlb/players.csv')
        os.remove('mlb/players.csv.backup')
