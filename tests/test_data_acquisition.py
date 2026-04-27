"""
Tests for data_acquisition.py
"""

from unittest.mock import Mock, patch
import pandas as pd

from migraine_weather import processing, data_acquisition


def test_get_eligible_stations(test_time):
    """
    Function to test get_eligible_stations
    """
    start, end = test_time

    eligible_stations = data_acquisition.get_eligible_stations(start, end)

    assert isinstance(eligible_stations, pd.DataFrame)
    assert not eligible_stations.empty
    for col in ("name", "country", "latitude", "longitude"):
        assert col in eligible_stations.columns


def test_get_variation_frac(test_data):
    """
    Function to test compute_frac_var
    """
    daily = processing.get_daily_pressure_range(test_data)
    frac_var_test = processing.compute_frac_var(daily)

    # test that output is of type float
    assert isinstance(frac_var_test, float)


def test_process_station_low_completeness():
    """
    Test that _process_station returns NaN for stations with <50% completeness.
    """
    # Create data with 60% missing values (40% completeness)
    dates = pd.date_range("2020-01-01", periods=100, freq="h")
    pressures = [1013.0] * 40 + [None] * 60

    df = pd.DataFrame({"pres": pressures}, index=dates)
    df.index.name = "time"
    df["station"] = "TEST01"
    df = df.set_index("station", append=True)

    result = data_acquisition._process_station(("TEST01", df), "TS")

    # Should return None due to low completeness
    assert result is None


def test_process_station_underreported_days():
    """
    Test that _process_station returns NaN for stations with >50% underreported days.
    """
    # Create 10 days where most days have <6 hourly readings
    dates = []
    pressures = []

    # 7 days with only 3 readings each (underreported)
    for day in range(7):
        day_dates = pd.date_range(f"2020-01-{day + 1:02d}", periods=3, freq="h")
        dates.extend(day_dates)
        pressures.extend([1013.0] * 3)

    # 3 days with full 24 readings (properly reported)
    for day in range(7, 10):
        day_dates = pd.date_range(f"2020-01-{day + 1:02d}", periods=24, freq="h")
        dates.extend(day_dates)
        pressures.extend([1013.0] * 24)

    df = pd.DataFrame({"pres": pressures}, index=dates)
    df.index.name = "time"
    df["station"] = "TEST02"
    df = df.set_index("station", append=True)

    result = data_acquisition._process_station(("TEST02", df), "TS")

    # Should return None due to >50% underreported days
    assert result is None


def test_make_dataset_filters_invalid_stations():
    """
    Test that make_dataset filters out stations with insufficient data early.
    """
    # Create mock station data
    station_data = pd.DataFrame(
        {"name": ["Station1", "Station2"], "latitude": [0.0, 1.0], "longitude": [0.0, 1.0]},
        index=["ST001", "ST002"],
    )

    # Mock hourly data where ST001 has enough data, ST002 doesn't
    mock_hourly = pd.DataFrame({"pres": [1013.0] * 100 + [1013.0] * 10})
    mock_hourly["station"] = ["ST001"] * 100 + ["ST002"] * 10
    mock_hourly.index = pd.date_range("2020-01-01", periods=110, freq="h")
    mock_hourly = mock_hourly.set_index("station", append=True)

    start = pd.Timestamp("2020-01-01")
    end = pd.Timestamp("2020-01-05")

    with patch("migraine_weather.data_acquisition.meteostat.hourly") as mock_hourly_fn:
        mock_ts = Mock()
        mock_ts.fetch.return_value = mock_hourly
        mock_hourly_fn.return_value = mock_ts

        result = data_acquisition.make_dataset("TS", station_data, start, end)

        # ST002 should be filtered out (only 10 readings < 50% of time range)
        assert "ST002" not in result
        # ST001 should remain
        assert "ST001" in result


def test_make_dataset_success():
    """
    Test that make_dataset works correctly with valid station data.
    """
    # Create mock station data
    station_data = pd.DataFrame(
        {"name": ["Station1"], "latitude": [0.0], "longitude": [0.0]}, index=["ST001"]
    )

    # Create mock hourly data with sufficient coverage
    dates = pd.date_range("2020-01-01", periods=365 * 24, freq="h")
    mock_hourly = pd.DataFrame({"pres": [1013.0] * len(dates)})
    mock_hourly["station"] = "ST001"
    mock_hourly.index = dates
    mock_hourly = mock_hourly.set_index("station", append=True)

    start = pd.Timestamp("2020-01-01")
    end = pd.Timestamp("2020-12-31")

    with patch("migraine_weather.data_acquisition.meteostat.hourly") as mock_hourly_fn:
        mock_ts = Mock()
        mock_ts.fetch.return_value = mock_hourly
        mock_hourly_fn.return_value = mock_ts

        result = data_acquisition.make_dataset("TS", station_data, start, end)

        # Station should be in result dict
        assert "ST001" in result
        # Value should be a DataFrame with daily min/max columns
        daily_df = result["ST001"]
        assert isinstance(daily_df, pd.DataFrame)
        assert "pres_min" in daily_df.columns
        assert "pres_max" in daily_df.columns
        assert "date" in daily_df.columns
