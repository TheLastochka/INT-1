import mysql.connector
import pytest
import os

from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("HOST")
USER= os.getenv("USERNAME")
PASSWORD= os.getenv("PASSWORD")

def get_connection():
    connection = mysql.connector.connect(
        host= HOST,
        user= USER,
        password= PASSWORD,
        database='int_1_test_db'
    )
    return connection

@pytest.fixture
def db_connection():
    connection = get_connection()
    yield connection
    connection.close()