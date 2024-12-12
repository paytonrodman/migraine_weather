# test_make_dataset.py

import pytest

import sys
sys.path.append("../..") # Adds higher directory to python modules path.

from migraine_weather.data import make_dataset
import pandas as pd
from datetime import datetime
import meteostat
import glob


def test_check_file_exists():
    """
    Function to test check_file_exists from migraine_weather.data.make_dataset
    """
    country_codes = make_dataset.get_country_codes() # get list of real country codes
    test_data_files = [i + '.csv' for i in country_codes] # make a mock list of data files
    test_data_files_regex = [d.split('/')[-1][:2] for d in test_data_files] # remove .csv from mock files
    test_cc_exists = 'AU' # country code that does exist
    test_cc_notexists = 'AA' # country code that does not exist

    # test that function returns 1 when overwrite_flag is 0 and file exists
    assert make_dataset.check_file_exists(test_cc_exists, test_data_files_regex, overwrite_flag=0) == 1
    # test that function returns 0 when overwrite_flag is 1 and file exists
    assert make_dataset.check_file_exists(test_cc_exists, test_data_files_regex, overwrite_flag=1) == 0
    # test that function returns 0 when non-existent cc is passed, regardless of overwrite_flag
    assert make_dataset.check_file_exists(test_cc_notexists, test_data_files_regex, overwrite_flag=0) == 0
    assert make_dataset.check_file_exists(test_cc_notexists, test_data_files_regex, overwrite_flag=1) == 0


def test_remove_outliers():
    """
    Function to test remove_outliers from migraine_weather.data.make_dataset
    """
    test_data = get_test_data()
    original_columns = set(test_data.columns)

    cleaned_data = make_dataset.remove_outliers(test_data)

    # test that the output has not lost any columns from the input
    assert set(cleaned_data.columns).issubset(test_data.columns)
    # test that the original data columns remain unaltered
    assert original_columns==set(test_data.columns)
    # test that the output is of type pd.DataFrame
    assert isinstance(cleaned_data, pd.DataFrame)

def test_get_variation_frac():
    """
    Function to test get_variation_frac from migraine_weather.data.make_dataset
    """
    test_data = get_test_data()
    frac_var_test = make_dataset.get_variation_frac(test_data)

    # test that output is of type float
    assert isinstance(frac_var_test, float)

def test_select_on_hours():
    """
    Function to test select_on_hours from migraine_weather.data.make_dataset
    """
    start, end = get_test_time()
    test_stations_input = get_test_stations()

    test_stations_output = make_dataset.select_on_hours(test_stations_input, start, end)

    # test that all start and end times encapsulate the correct range
    test_times = (test_stations_output['hourly_start'].dt.year<=start.year) & (test_stations_output['hourly_end'].dt.year>=end.year).all()
    assert test_times.all()
    # test that the function output is of type pd.DataFrame
    assert isinstance(test_stations_output, pd.DataFrame)
    # test that the output retains all the same columns as the input
    assert set(test_stations_input.columns).issubset(test_stations_output.columns)

def test_get_country_codes():
    """
    Function to test get_country_codes from migraine_weather.data.make_dataset
    """
    cc = make_dataset.get_country_codes()
    # test that output is of type list
    assert isinstance(cc, list)
    # test that all entries have 2 letters
    lengths = [len(i) for i in cc]
    assert len(list(set(lengths)))==1
    assert list(set(lengths))[0]==2

def get_test_stations():
    """
    Provides test station data for a single country (Australia, AU).
    """
    return meteostat.Stations().region('AU').fetch()

def get_test_data():
    """
    Provides test weather data for a single station (Canberra).
    """
    start, end = get_test_time()
    test_stations = get_test_stations()
    test_station = test_stations[test_stations.name == 'Canberra']
    return meteostat.Hourly(test_station.index[0], start, end).fetch()

def get_test_time():
    """
    Provides a test time range.
    """
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)
    return start, end
