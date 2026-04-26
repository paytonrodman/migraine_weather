"""
Tests for make_dataset.py
"""

import pandas as pd

from migraine_weather import processing, data_acquisition


def test_get_eligible_stations(test_time):
    """
    Function to test get_eligible_stations from migraine_weather.make_dataset
    """
    start, end = test_time
    freq = "Hourly"

    eligible_stations = data_acquisition.get_eligible_stations(freq, start, end)

    # test that all start and end times encapsulate the correct range
    test_times = (eligible_stations[freq.lower() + "_start"].dt.year <= start.year) & (
        eligible_stations[freq.lower() + "_end"].dt.year >= end.year
    ).all()
    assert test_times.all()
    # test that the output is of type pd.DataFrame
    assert isinstance(eligible_stations, pd.DataFrame)


def test_get_variation_frac(test_data):
    """
    Function to test get_variation_frac from migraine_weather.make_dataset
    """
    frac_var_test = processing.get_variation_frac(test_data)

    # test that output is of type float
    assert isinstance(frac_var_test, float)
