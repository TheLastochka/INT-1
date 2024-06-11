from db_connection import db_connection
import os

DEBUG = os.getenv("DEBUG", False)

SQL_PATTERN_USED_INDEX = 'test_text%'
SQL_PATTERN_NOT_USED_INDEX = '%test_text%'

def create_performance_used_index(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("CREATE FULLTEXT INDEX performance_index ON performance_table (text);")
    db_connection.commit()
    cursor.close()

def create_performance_not_used_index(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("CREATE FULLTEXT INDEX performance_index ON performance_table (text);")
    db_connection.commit()
    cursor.close()

def drop_if_exist_performance_index(db_connection):
    cursor = db_connection.cursor()
    try:
        cursor.execute("DROP INDEX performance_index ON performance_table;")
        db_connection.commit()
    except Exception as e:
        pass
    cursor.close()

def get_last_query_time(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SHOW PROFILE;")
    result = cursor.fetchall()
    cursor.close()
    summ = 0
    for item in result:
        summ += item[1]
    return summ

def profiling_on(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SET profiling = 1;")
    cursor.close()
    db_connection.commit()

def performance_str_like_pattern_test(db_connection, pattern, used_index):
    profiling_on(db_connection)
    cursor = db_connection.cursor()

    drop_if_exist_performance_index(db_connection)

    if DEBUG:
        test_query = f"EXPLAIN SELECT text FROM performance_table WHERE text LIKE '{pattern}';"
    else:
        test_query = f"SELECT text FROM performance_table WHERE text LIKE '{pattern}';"

    # select like PATTERN without index
    cursor.execute(test_query)
    result_no_index = cursor.fetchall()
    time_no_index = get_last_query_time(db_connection)
    if DEBUG: print("\n", result_no_index)

    if used_index:
        create_performance_used_index(db_connection)
    else:
        create_performance_not_used_index(db_connection)

    # select like PATTERN with index
    cursor.execute(test_query)
    result_with_index = cursor.fetchall()
    time_with_index = get_last_query_time(db_connection)
    if DEBUG: print("\n", result_with_index)

    print()
    print(f"Time index no:\t{time_no_index}")
    print(f"Time index yes:\t{time_with_index}")
    print(f"subs (no - yes): {time_no_index - time_with_index}")

    # sort results
    result_no_index = sorted(result_no_index)
    result_with_index = sorted(result_with_index)

    cursor.close()
    return time_no_index, time_with_index, result_no_index, result_with_index
    

def test_performance_index_used(db_connection):
    pattern = SQL_PATTERN_USED_INDEX
    time_no_index, time_with_index, result_no_index, result_with_index = performance_str_like_pattern_test(db_connection, pattern, True)
    db_connection.close()

    assert time_no_index > time_with_index
    
def test_performance_index_not_used(db_connection):
    pattern = SQL_PATTERN_NOT_USED_INDEX
    time_no_index, time_with_index, result_no_index, result_with_index = performance_str_like_pattern_test(db_connection, pattern, False)
    db_connection.close()

    assert 0.01 >= abs(time_no_index - time_with_index)
    
    



