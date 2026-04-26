"""
Tests for processing.py
"""

import pandas as pd

from migraine_weather import processing


def test_remove_outliers(test_data: pd.DataFrame):
    """
    Function to test remove_outliers from migraine_weather.make_dataset
    """
    original_columns = set(test_data.columns)

    cleaned_data = processing.remove_outliers(test_data)

    # test that the output has not lost any columns from the input
    assert set(cleaned_data.columns).issubset(test_data.columns)
    # test that the original data columns remain unaltered
    assert original_columns == set(test_data.columns)
    # test that the output is of type pd.DataFrame
    assert isinstance(cleaned_data, pd.DataFrame)


def test_get_variation_frac(test_data: pd.DataFrame):
    """
    Function to test get_variation_frac from migraine_weather.processing
    """
    frac_var_test = processing.get_variation_frac(test_data)

    # test that output is of type float
    assert isinstance(frac_var_test, float)
