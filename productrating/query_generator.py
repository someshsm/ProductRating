def prepare_column_query(column_properties: dict) -> str:
    """
    To dynamically prepare query for columns and its properties as per dictionary.
    :param column_properties:
    :return:
    """
    values = ""
    for c in column_properties:
        values += f"{c} "
        properties = column_properties.get(c)
        values += f"{" ".join(properties)}, "

    return values[:-2]


def prepare_create_table_query(table_name: str,
                               column_properties: dict) -> str:
    """
    To dynamically prepare query for creating a table.
    :param table_name:
    :param column_properties:
    :return:
    """
    column_values_query = prepare_column_query(column_properties)
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({column_values_query});"


def append_place_holder(count: int) -> str:
    """
    To dynamically prepare placeholder query.
    :param count:
    :return:
    """
    value = ""
    for _ in range(count):
        value += "?,"
    return value[:-1]


def prepare_insert_raw_data_query(table_name: str,
                                  column_names: list) -> str:
    """
    To dynamically prepare query to insert data.
    :param table_name:
    :param column_names:
    :return:
    """
    append_column_names = f"{', '.join(column_names)}"
    append_place_holders = append_place_holder(len(column_names))
    return f"INSERT INTO {table_name} ({append_column_names}) VALUES ({append_place_holders});"