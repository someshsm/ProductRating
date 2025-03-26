import sqlite3
from typing import Generator, Any


def create_table(query: str,
                 db_name) -> None:
    """
    To create a table.
    :param db_name:
    :param query:
    :return:
    """
    try:
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
    except sqlite3.Error as e:
        print("Error creating table")
        raise Exception("sqlite3.Error") from e


def validate_if_table_exists(table_name: str,
                             db_name) -> str:
    """
    To validate if table is created
    :param db_name:
    :param table_name:
    :return:
    """
    query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    try:
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                return f"{table_name} table created"
            else:
                return f"{table_name} table doesn't exists"
    except sqlite3.Error as e:
        print("Error validating table")
        raise Exception("sqlite3.Error") from e


def insert_data(query: str,
                data_generator: Generator[Any, None, None],
                db_name) -> None:
    """
    To insert data into table
    :param query:
    :param data_generator:
    :param db_name:
    :return:
    """
    try:
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.executemany(query, data_generator)
            connection.commit()
    except sqlite3.Error as e:
        print("Error inserting data")
        raise Exception("sqlite3.Error") from e


def validate_if_data_inserted(table_name: str,
                              db_name) -> str:
    """
    Verifies if all the data is inserted into the table
    :param table_name:
    :param db_name:
    :return:
    """
    query = f"SELECT count(*) from {table_name};"
    try:
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            return f"{result} number of records inserted into {table_name}"
    except sqlite3.Error as e:
        print("Error validating data")
        raise Exception("sqlite3.Error") from e


def compute_monthly_aggregate(db_name) -> list:
    """
    Calculates the avg rating for products on monthly basis.
    :param db_name:
    :return:
    """
    query = "SELECT strftime('%Y_%m', timestamp) as Month, product_id as ProductId, AVG(rating) as AverageRating FROM Ratings GROUP BY strftime('%Y_%m', timestamp), product_id ORDER BY Month;"
    try:
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return list(result)
    except sqlite3.Error as e:
        print("Error computing monthly aggregate")
        raise Exception("sqlite3.Error") from e


def fetch_top_products_of_month(month: str,
                                db_name):
    """
    Finds top 3 products based on avg rating for specified month
    :param month:
    :param db_name:
    :return:
    """
    query = f"SELECT product_id, {month} FROM RatingsMonthlyAggregate ORDER BY {month} DESC LIMIT 3;"
    try:
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    except sqlite3.Error as e:
        print("Error fetching top products")
        raise Exception("sqlite3.Error") from e
