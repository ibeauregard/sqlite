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
    select_queries = [('select * from players join batting on a,b,c', 'Syntax error'),

                      ('select * from players where id=42', 'Syntax error'),

                      ('select * from players where id=42=24', 'Syntax error'),

                      ('select * from players where id', 'Syntax error'),

                      ('select * from players join batting, pitching', 'Syntax error'),

                      ('select * from players order by id ???', 'Syntax error'),

                      ('select * from players order by id namelast', 'Syntax error'),

                      ('select * from players limit ten', 'Syntax error'),

                      ('SELECT FROM players', 'Syntax error'),

                      ('SELECT * FROM players LIMIT 10', 'Success'),

                      ('SELECT namelast, namefirst FROM players limit 10', 'Success'),

                      ('SELECT players.namelast, players.namefirst from players limit 10', 'Success'),

                      ('SELECT * FROM players WHERE id="0"', 'Success'),

                      ('SELECT namelast, namefirst, yearid, hr '
                       'FROM players join batting on batting.playerid = players.id '
                       'order by hr desc, namelast, namefirst, yearid limit 10', 'Success')]

    update_queries = [('update players', 'Syntax error'),

                      ('update players, batting set id = "42"', 'Syntax error'),

                      ('update players set a = b', 'Syntax error'),

                      ('update players set id = 1000', 'Syntax error'),

                      ('update players set id = "42", nameGiven: "Roger"', 'Syntax error'),

                      ('update players set (birthcountry = "USofA", deathcountry = "Canada") '
                       'where birthcountry = "USA"', 'Syntax error'),

                      ('update players set id = "42000" where id = "42"', 'Success'),

                      ('update players set birthcountry = "USofA", deathcountry = "Canada" '
                       'where birthcountry = "USA"', 'Success')]

    insert_queries = [('insert into players', 'Syntax error'),

                      ('insert into players, players (id) values ("100")', 'Syntax error'),

                      ('insert into players (id, namelast, namefirst) values a, b, c', 'Syntax error'),

                      ('insert into players (id, namelast, namefirst) values (a, b, c), (d, e, f)', 'Syntax error'),

                      ('insert into players (id, namelast, namefirst) values (500, Roger, Clemens)', 'Syntax error'),

                      ('insert into players (id, namelast, namefirst) '
                       'values ("999", "Roger", "Clemens"), ("1000", "Roger")', 'Insert error'),

                      ('insert into players (id) values ("999", "Roger"), ("1000", "Roger")', 'Insert error'),

                      ('insert into players (id) values ("9991")', 'Success'),

                      ('insert into players (id) values ("9992"), ("9993")', 'Success'),

                      ('insert into players (id, namelast, namefirst) '
                       'values ("9994", "Roger", "Clemens")', 'Success'),

                      ('insert into players (id, namelast, namefirst) '
                       'values ("9995", "Roger", "Clemens"), ("9996", "Jackie", "Robinson")', 'Success'),

                      ('insert into players values ("9997", "", "", "", "", "", "", "", '
                       '"", "", "", "", "", "", "", '
                       '"", "", "", "", "", "", "")', 'Success'),

                      ('insert into players values ("9998", "", "", "", "", "", "", "", '
                       r'"", "", "", "You can \"also\" insert quotes", "", "", "", '
                       '"", "", "", "", "", "", ""),'
                       ''
                       '("9999", "", "", "", "", "", "", "", '
                       '"", "", "", "and control characters, such as\n the line feed", "", "", "", '
                       '"", "", "", "", "", "", "")', 'Success')]

    delete_queries = [('delete players where id=42', 'Syntax error'),

                      ('delete players', 'Syntax error'),

                      ('delete from players where id=42', 'Syntax error'),

                      ('delete from players where id=42=24', 'Syntax error'),

                      ('delete from players where id', 'Syntax error'),

                      ('delete from players where id = "919"', 'Success'),

                      ('delete from players', 'Success')]

    shutil.copy2('mlb/players.csv', 'mlb/players.csv.backup')
    try:
        TestSuite.run(select_queries)
        TestSuite.run(update_queries)
        TestSuite.run(insert_queries)
        TestSuite.run(delete_queries)
    finally:
        shutil.copy2('mlb/players.csv.backup', 'mlb/players.csv')
        os.remove('mlb/players.csv.backup')
