
from db_connection import db_connection

SQL_PATTERN = 'test_text%'

def create_functional_index(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("CREATE INDEX functional_index ON functional_table (text);")
    db_connection.commit()
    cursor.close()

def drop_if_exist_functional_index(db_connection):
    cursor = db_connection.cursor()
    try:
        cursor.execute("DROP INDEX functional_index ON functional_table;")
        db_connection.commit()
    except Exception as e:
        pass
    cursor.close()


def test_functional_is_same_with_index(db_connection):
    cursor = db_connection.cursor()

    drop_if_exist_functional_index(db_connection)

    test_query = f"SELECT text FROM functional_table WHERE text LIKE '{SQL_PATTERN}';"

    # select like PATTERN without index
    cursor.execute(test_query)
    result_no_index = cursor.fetchall()

    create_functional_index(db_connection)

    # select like PATTERN with index
    cursor.execute(test_query)
    result_with_index = cursor.fetchall()

    assert result_no_index == result_with_index