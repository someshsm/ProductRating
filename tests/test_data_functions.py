from unittest import mock
from datetime import date, timedelta

from productrating.main import (raw_data_generator, structure_computed_data, aggregate_data_generator)


@mock.patch('random.randint')
def test_raw_data_generator(mock_randint):
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)

    mock_randint.side_effect = [10, 3, 4, 5, 13, 4, 5, 4]
    generated_list = list(raw_data_generator(2, 10, 10, 5, start_date, end_date))

    expected = [(start_date + timedelta(10), 3, 4, 5), (start_date + timedelta(13), 4, 5, 4)]

    assert generated_list == expected


def test_structure_computed_data():
    values = [["2024_04", 1, 4], ["2024_05", 1, 4.5]]

    result = structure_computed_data(values)
    expected = {1: {"2024_04": 4, "2024_05": 4.5}}
    assert result == expected


def test_aggregate_data_generator():
    value = {1: {"2024_04": 4, "2024_05": 4.5}}

    result = list(aggregate_data_generator(value))
    expected = [[1, 0, 0, 0, 4, 4.5, 0, 0, 0, 0, 0, 0, 0]]
    assert result == expected
