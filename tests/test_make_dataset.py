"""
Tests for make_dataset.py
"""

import pytest

from migraine_weather import processing
import pandas as pd
from datetime import datetime
import meteostat


@pytest.mark.parametrize(
    "country_code,overwrite_flag,file_exists",
    [
        ("AU", False, True),
        ("AU", True, False),
        ("AA", False, False),
        ("AA", True, False)
    ],
    ids=("no_overwrite_valid_cc", "overwrite_valid_cc", "no_overwrite_invalid_cc", "overwrite_invalid_cc")
)
def test_check_file_exists(country_code: str, overwrite_flag: bool, file_exists: bool):
    """
    Function to test check_file_exists from migraine_weather.make_dataset
    """
    country_codes = processing.get_country_codes()  # get list of real country codes
    test_data_files = [i + '.csv' for i in country_codes]  # make a mock list of data files
    test_data_files_regex = [d.split('/')[-1][:2] for d in test_data_files]  # remove .csv from mock files

    assert processing.check_file_exists(country_code, test_data_files_regex, overwrite=overwrite_flag) == file_exists


def test_get_eligible_stations():
    """
    Function to test get_eligible_stations from migraine_weather.make_dataset
    """
    start, end = get_test_time()
    freq = 'Hourly'

    eligible_stations = processing.get_eligible_stations(freq, start, end)

    # test that all start and end times encapsulate the correct range
    test_times = (eligible_stations[freq.lower() + '_start'].dt.year <= start.year) & (eligible_stations[freq.lower() + '_end'].dt.year >= end.year).all()
    assert test_times.all()
    # test that the output is of type pd.DataFrame
    assert isinstance(eligible_stations, pd.DataFrame)


def test_remove_outliers():
    """
    Function to test remove_outliers from migraine_weather.make_dataset
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
    Function to test get_variation_frac from migraine_weather.make_dataset
    """
    test_data = get_test_data()
    frac_var_test = processing.get_variation_frac(test_data)

    # test that output is of type float
    assert isinstance(frac_var_test, float)


def test_get_country_codes():
    """
    Function to test get_country_codes from migraine_weather.make_dataset
    """
    cc = processing.get_country_codes()
    # test that output is of type list
    assert isinstance(cc, list)
    # test that all entries have 2 letters
    lengths = [len(i) for i in cc]
    assert len(list(set(lengths))) == 1
    assert list(set(lengths))[0] == 2


def get_test_stations():
    """
    Provides test station data for a single country (Australia, AU).
    """
    au_stations = meteostat.Stations().region('AU').fetch()

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
