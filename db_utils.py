# db_utils.py
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to SQLite version: {sqlite3.version}")
        return conn
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def get_all_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [table[0] for table in cursor.fetchall()]

def get_table_info(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    return cursor.fetchall()
