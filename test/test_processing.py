"""
Tests for processing.py
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from migraine_weather import processing
import meteostat


@pytest.mark.parametrize(
    "country_code,overwrite_flag,file_exists",
    [("AU", False, True), ("AU", True, False), ("AA", False, False), ("AA", True, False)],
    ids=(
        "no_overwrite_valid_cc",
        "overwrite_valid_cc",
        "no_overwrite_invalid_cc",
        "overwrite_invalid_cc",
    ),
)
def test_check_file_exists(country_code: str, overwrite_flag: bool, file_exists: bool):
    """
    Function to test check_file_exists from migraine_weather.processing
    """
    country_codes = processing.get_country_codes()
    test_data_files = [i + ".csv" for i in country_codes]
    test_data_files_regex = [d.split("/")[-1][:2] for d in test_data_files]

    assert (
        processing.check_file_exists(country_code, test_data_files_regex, overwrite=overwrite_flag)
        == file_exists
    )


def test_get_eligible_stations():
    """
    Function to test get_eligible_stations from migraine_weather.processing
    """
    start, end = get_test_time()
    freq = "Hourly"

    eligible_stations = processing.get_eligible_stations(freq, start, end)

    # test that all start and end times encapsulate the correct range
    test_times = (eligible_stations[freq.lower() + "_start"].dt.year <= start.year) & (
        eligible_stations[freq.lower() + "_end"].dt.year >= end.year
    ).all()
    assert test_times.all()
    # test that the output is of type pd.DataFrame
    assert isinstance(eligible_stations, pd.DataFrame)


def test_remove_outliers():
    """
    Function to test remove_outliers from migraine_weather.processing
    """
    test_data = get_test_data()
    original_columns = set(test_data.columns)

    cleaned_data = processing.remove_outliers(test_data)

    # test that the output has not lost any columns from the input
    assert set(cleaned_data.columns).issubset(test_data.columns)
    # test that the original data columns remain unaltered
    assert original_columns == set(test_data.columns)
    # test that the output is of type pd.DataFrame
    assert isinstance(cleaned_data, pd.DataFrame)


def test_get_variation_frac():
    """
    Function to test get_variation_frac from migraine_weather.processing
    """
    test_data = get_test_data()
    frac_var_test = processing.get_variation_frac(test_data)

    # test that output is of type float
    assert isinstance(frac_var_test, float)


def test_get_country_codes():
    """
    Function to test get_country_codes from migraine_weather.processing
    """
    cc = processing.get_country_codes()
    # test that output is of type list
    assert isinstance(cc, list)
    # test that all entries have 2 letters
    lengths = [len(i) for i in cc]
    assert len(list(set(lengths))) == 1
    assert list(set(lengths))[0] == 2


def test_compile_data():
    """
    Function to test compile_data from migraine_weather.processing
    """
    with TemporaryDirectory() as tmpdir:
        input_path = Path(tmpdir) / "input"
        output_path = Path(tmpdir) / "output"
        input_path.mkdir()
        output_path.mkdir()

        df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        df2 = pd.DataFrame({"id": [3, 4], "value": [30, 40]})
        df1.to_csv(input_path / "file1.csv", index=False)
        df2.to_csv(input_path / "file2.csv", index=False)

        processing.compile_data(input_path, output_path)

        result = pd.read_csv(output_path / "all.csv")
        # test that output contains all rows
        assert len(result) == 4
        # test that output is of type pd.DataFrame
        assert isinstance(result, pd.DataFrame)


def get_test_stations():
    """
    Provides test station data for a single country (Australia, AU).
    """
    au_stations = meteostat.Stations().region("AU").fetch()

    return au_stations


def get_test_data():
    """
    Provides test weather data for a single station (Canberra).
    """
    start, end = get_test_time()
    test_stations = get_test_stations()
    station_id = test_stations.loc[test_stations["name"] == "Canberra"].index.item()

    hourly = meteostat.Hourly(station_id, start, end).fetch()

    return hourly


def get_test_time():
    """
    Provides a test time range.
    """
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)
    return start, end
