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


def test_remove_outliers_removes_outliers():
    """
    Test that remove_outliers actually removes days with outlier pressure spikes.
    """
    # Create hourly data for 3 days with a major outlier spike on day 2
    dates = pd.date_range("2020-01-01", periods=72, freq="h")
    pressures = [1013.0] * 72  # Normal pressure

    # Add outlier spikes on day 2 (hours 24-47) - 3 spikes
    pressures[26] = 1050.0  # +37 hPa spike
    pressures[30] = 980.0  # -33 hPa spike
    pressures[35] = 1045.0  # +32 hPa spike

    df = pd.DataFrame({"pres": pressures}, index=dates)

    cleaned = processing.remove_outliers(df)

    # Day 2 should be removed (has >=2 outliers)
    cleaned_dates = pd.DatetimeIndex(cleaned.index).strftime("%Y-%m-%d")
    assert "2020-01-02" not in cleaned_dates
    # Days 1 and 3 should remain
    assert "2020-01-01" in cleaned_dates
    assert "2020-01-03" in cleaned_dates


def test_get_variation_frac_calculation():
    """
    Test that get_variation_frac correctly calculates fraction of high variation days.
    """
    # Create 2 years of daily data
    # Year 1: 10 days with high variation (>10 hPa), 355 days low variation
    # Year 2: 20 days with high variation, 345 days low variation
    dates_y1 = pd.date_range("2020-01-01", periods=365 * 24, freq="h")
    dates_y2 = pd.date_range("2021-01-01", periods=365 * 24, freq="h")

    pressures_y1 = [1013.0] * (365 * 24)
    pressures_y2 = [1013.0] * (365 * 24)

    # Add high variation days (>10 hPa range within day)
    # Year 1: 10 days
    for day in range(10):
        pressures_y1[day * 24] = 1000.0
        pressures_y1[day * 24 + 12] = 1015.0  # 15 hPa variation

    # Year 2: 20 days
    for day in range(20):
        pressures_y2[day * 24] = 1000.0
        pressures_y2[day * 24 + 12] = 1015.0  # 15 hPa variation

    df = pd.DataFrame({"pres": pressures_y1 + pressures_y2}, index=dates_y1.append(dates_y2))

    frac_var = processing.get_variation_frac(df)

    # Expected: (10/365 + 20/365) / 2 ≈ 0.041
    assert 0.035 < frac_var < 0.045
