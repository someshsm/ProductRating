import random
from datetime import date, timedelta
from typing import Generator, Tuple

from productrating.data_base_functions import (create_table,
                                               validate_if_table_exists,
                                               insert_data,
                                               validate_if_data_inserted,
                                               compute_monthly_aggregate,
                                               fetch_top_products_of_month)
from productrating.query_generator import (prepare_create_table_query,
                                           prepare_insert_raw_data_query)

# Constants
DB_NAME = "ProductRating.db"
NO_OF_RECORDS = 100000
START_DATE = date(2024, 1, 1)
END_DATE = date(2024, 12, 31)
MAX_USER_ID = 1000
MAX_PRODUCT_ID = 1000
MAX_RATING_VALUE = 5
RAW_DATA_TABLE_NAME = "Ratings"
AGGREGATE_DATA_TABLE_NAME = "RatingsMonthlyAggregate"

RAW_TABLE_COLUMN_PROPERTIES = {
    "timestamp": ["DATE"],
    "user_id": ["INT"],
    "product_id": ["INT"],
    "rating": ["INT"]
}


def raw_data_generator(total_records: int, max_user_id: int, max_product_id: int, max_rating : int, start_date: date, end_date: date) -> \
Generator[Tuple[date, int, int, int]]:
    """
    Generates raw data for Ratings Table using random and timedelta.
    :param total_records:
    :param max_user_id:
    :param max_product_id:
    :param max_rating:
    :param start_date:
    :param end_date:
    :return:
    """
    # Setting constant num to generate same random values.
    random.seed(20)
    difference = (end_date - start_date).days

    for _ in range(total_records):
        delta_num = random.randint(0, difference)
        timestamp = start_date + timedelta(delta_num)
        user_id = random.randint(1, max_user_id)
        product_id = random.randint(1, max_product_id)
        rating = random.randint(1, max_rating)
        yield timestamp, user_id, product_id, rating


def structure_computed_data(data: list) -> dict:
    """
    Structuring computed data into dictionary where key is product and value is monthly ratings as nested dictionary.
    :param data:
    :return:
    """
    structured_data = {}
    for d in data:
        timestamp = d[0]
        product_id = d[1]
        avg_rating = round(d[2], 2)
        new_value = {timestamp: avg_rating}

        existing_value = structured_data.get(product_id)
        if existing_value:
            existing_value.update(new_value)
        else:
            existing_value = new_value

        structured_data.update({product_id: existing_value})

    return structured_data


def aggregate_table_columns_properties():
    """
    Returns the column names and its properties for RatingsMonthlyAggregate Table.
    :return:
    """
    return {
        "product_id": ["INT", "NOT NULL", "UNIQUE"],
        "Jan2024": ["DECIMAL(10, 2)"],
        "Feb2024": ["DECIMAL(10, 2)"],
        "Mar2024": ["DECIMAL(10, 2)"],
        "Apr2024": ["DECIMAL(10, 2)"],
        "May2024": ["DECIMAL(10, 2)"],
        "Jun2024": ["DECIMAL(10, 2)"],
        "Jul2024": ["DECIMAL(10, 2)"],
        "Aug2024": ["DECIMAL(10, 2)"],
        "Sep2024": ["DECIMAL(10, 2)"],
        "Oct2024": ["DECIMAL(10, 2)"],
        "Nov2024": ["DECIMAL(10, 2)"],
        "Dec2024": ["DECIMAL(10, 2)"]
    }


def aggregate_data_generator(structured_data: dict):
    """
    Aggregated date is organized/structured as per RatingsMonthlyAggregator table schema.
    :param structured_data:
    :return:
    """
    months = ["2024_01", "2024_02", "2024_03", "2024_04", "2024_05", "2024_06",
              "2024_07", "2024_08", "2024_09",
              "2024_10", "2024_11", "2024_12"]

    for product_id in structured_data.keys():
        values = structured_data.get(product_id)
        values_key = list(values.keys())
        sub_values = [product_id]
        for m in months:
            if m in values_key:
                sub_values.append(values.get(m))
            else:
                sub_values.append(0.0)

        yield sub_values


def find_top_products_for_all_months() -> dict:
    """
    Returns top 3 products of all the months based on avg rating.
    :return:
    """
    result = {}
    aggregate_columns = aggregate_table_columns_properties()
    columns = list(aggregate_columns.keys())
    columns.pop(0)
    for m in columns:
        res = fetch_top_products_of_month(m, DB_NAME)
        result.update({m: res})
    return result


def show_result(result):
    """
    Prints the top 3 products and its avg rating with respect to month.
    :param result:
    :return:
    """
    for key in result:
        print(f"Top 3 Products of {key}:")
        values = result.get(key)
        for v in values:
            print(f"Product ID : {v[0]} , Avg Rating: {v[1]}")
        print("")


def main():
    """
    Creates Ratings table and inserts data.
    Computes monthly aggregate.
    Creates RatingsMonthlyAggregate table and inserts computed data.
    Finds top 3 products for each month and displays the results.
    :return:
    """
    # Create Ratings table
    raw_table_query = prepare_create_table_query(RAW_DATA_TABLE_NAME,
                                                 RAW_TABLE_COLUMN_PROPERTIES)
    create_table(raw_table_query,
                 db_name=DB_NAME)
    print(validate_if_table_exists(RAW_DATA_TABLE_NAME,
                                   db_name=DB_NAME))

    # Insert random data to Ratings table
    raw_data_insert_query = prepare_insert_raw_data_query(RAW_DATA_TABLE_NAME,
                                                          list(RAW_TABLE_COLUMN_PROPERTIES.keys()))
    insert_data(raw_data_insert_query,
                raw_data_generator(NO_OF_RECORDS,
                                   MAX_USER_ID,
                                   MAX_PRODUCT_ID,
                                   MAX_RATING_VALUE,
                                   START_DATE,
                                   END_DATE),
                db_name=DB_NAME)
    print(validate_if_data_inserted(RAW_DATA_TABLE_NAME,
                                    db_name=DB_NAME))

    # Compute results
    computed_result = compute_monthly_aggregate(db_name=DB_NAME)
    structured_data = structure_computed_data(computed_result)

    # Create RatingsMonthlyAggregate table
    aggregate_table_properties = aggregate_table_columns_properties()
    aggregate_table_query = prepare_create_table_query(AGGREGATE_DATA_TABLE_NAME,
                                                       aggregate_table_properties)
    create_table(aggregate_table_query,
                 db_name=DB_NAME)
    print(validate_if_table_exists(AGGREGATE_DATA_TABLE_NAME,
                                   db_name=DB_NAME))

    # Insert data into RatingsMonthlyAggregate table
    aggregate_data_insert_query = prepare_insert_raw_data_query(AGGREGATE_DATA_TABLE_NAME,
                                                                list(aggregate_table_properties.keys()))
    aggregate_data = aggregate_data_generator(structured_data)
    insert_data(aggregate_data_insert_query,
                aggregate_data,
                db_name=DB_NAME)
    print(validate_if_data_inserted(AGGREGATE_DATA_TABLE_NAME,
                                    db_name=DB_NAME))

    # Compute top 3 products for all months
    top_products = find_top_products_for_all_months()
    show_result(top_products)


if __name__ == "__main__":
    main()
