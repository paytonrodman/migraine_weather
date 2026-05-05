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


def test_compute_frac_var(test_data: pd.DataFrame):
    """
    Function to test compute_frac_var from migraine_weather.processing
    """
    daily = processing.get_daily_pressure_range(test_data)
    frac_var_test = processing.compute_frac_var(daily)

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


def test_compute_frac_var_calculation():
    """
    Test that compute_frac_var correctly calculates fraction of high variation days.
    """
    # Build daily min/max directly: 2 years
    # Year 1: 10 high-variation days, Year 2: 20 high-variation days
    dates_y1 = pd.date_range("2020-01-01", periods=365, freq="D")
    dates_y2 = pd.date_range("2021-01-01", periods=365, freq="D")

    pres_min = [1013.0] * 730
    pres_max = [1013.0] * 730

    for day in range(10):  # Year 1: 10 high-variation days
        pres_min[day] = 1000.0
        pres_max[day] = 1015.0  # 15 hPa range

    for day in range(20):  # Year 2: 20 high-variation days
        pres_min[365 + day] = 1000.0
        pres_max[365 + day] = 1015.0

    daily_df = pd.DataFrame(
        {
            "date": list(dates_y1) + list(dates_y2),
            "pres_min": pres_min,
            "pres_max": pres_max,
        }
    )

    frac_var = processing.compute_frac_var(daily_df)

    # Expected: (10/365 + 20/365) / 2 ≈ 0.041
    assert 0.035 < frac_var < 0.045
