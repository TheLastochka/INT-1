
import pytest

from db_connection import get_connection
import os
DEBUG = os.getenv("DEBUG", False)

def fill_functional(db_connection):
    cursor = db_connection.cursor()

    # clear table
    cursor.execute("DELETE FROM functional_table;")
    db_connection.commit()

    # fill table
    insert_query = "INSERT INTO functional_table (id, text) VALUES "
    values = [
        (1, 'test_text'),
        (2, 'test_text2'),
        (3, 'test_text3')
    ]
    for value in values:
        cursor.execute(f"{insert_query}{value};")
    db_connection.commit()
    cursor.close()

import random
import time

def fill_performance(db_connection):
    cursor = db_connection.cursor()

    # clear table
    cursor.execute("DELETE FROM performance_table;")
    db_connection.commit()

    # fill table
    random.seed(time.time())
    values = []
    for i in range(1, 20_000):
        random_part = ''.join(random.choices('0123456789abcdefghijklmnopqrstuvwxyz', k=10))
        if i %23 == 0:
            values.append((i, f'test_text{random_part}'))
        else:
            values.append((i, f'test_false{random_part}'))


    insert_query = "INSERT INTO performance_table (id, text) VALUES "
    parts = 10
    for i in range(0, len(values), parts):
        part_values = values[i:i+parts]
        cursor.execute(f"{insert_query}{','.join([str(value) for value in part_values])};")
    
    db_connection.commit()
    cursor.close()


def init_db():
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS functional_table (id INT, text VARCHAR(100));")
    cursor.execute("CREATE TABLE IF NOT EXISTS performance_table (id INT, text VARCHAR(100));")

    fill_functional(connection)
    fill_performance(connection)

    cursor.close()
    connection.close()


if __name__ == "__main__":
    init_db()
    
    if DEBUG:
        pytest.main(['-v', '-s', 'functional_test.py'])
        pytest.main(['-v', '-s', 'performance_test.py'])
    else:
        pytest.main(['-v', 'functional_test.py'])
        pytest.main(['-v', 'performance_test.py'])



