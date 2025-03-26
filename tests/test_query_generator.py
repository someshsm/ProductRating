from productrating.query_generator import (prepare_column_query,
                                           prepare_create_table_query,
                                           append_place_holder,
                                           prepare_insert_raw_data_query)


def test_prepare_column_query():
    column_properties = {
        "product_name": ["VARCHAR(255)", "NOT NULL", "UNIQUE"],
        "product_id": ["INT", "NOT NULL"],
        "timestamp": ["DATE", "NOT NULL"]
    }

    result = prepare_column_query(column_properties)
    expected = "product_name VARCHAR(255) NOT NULL UNIQUE, product_id INT NOT NULL, timestamp DATE NOT NULL"
    assert result == expected


def test_prepare_create_table_query():
    table_name = "Testing"
    column_properties = {
        "product_name": ["VARCHAR(255)", "NOT NULL", "UNIQUE"],
        "product_id": ["INT", "NOT NULL"],
        "timestamp": ["DATE", "NOT NULL"]
    }

    result = prepare_create_table_query(table_name, column_properties)
    expected = f"CREATE TABLE IF NOT EXISTS {table_name} (product_name VARCHAR(255) NOT NULL UNIQUE, product_id INT NOT NULL, timestamp DATE NOT NULL);"
    assert result == expected


def test_append_place_holder():
    count = 5

    result = append_place_holder(count)
    expected = "?,?,?,?,?"
    assert result == expected


def test_prepare_insert_raw_data_query():
    table_name = "Testing"
    column_names = ["product_id", "user_id", "rating"]

    result = prepare_insert_raw_data_query(table_name, column_names)
    expected = f"INSERT INTO {table_name} (product_id, user_id, rating) VALUES (?,?,?);"
    assert result == expected
