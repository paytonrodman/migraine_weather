"""
Tests for data_acquisition.py
"""

from unittest.mock import Mock, patch
from datetime import datetime
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
    Test that _process_station returns None for stations with <50% completeness.
    """
    dates = pd.date_range("2020-01-01", periods=100, freq="h")
    pressures = [1013.0] * 40 + [None] * 60
    mock_df = pd.DataFrame({"pres": pressures}, index=dates)

    start = datetime(2020, 1, 1)
    end = datetime(2020, 1, 5)

    with patch("migraine_weather.data_acquisition.meteostat.hourly") as mock_hourly_fn:
        mock_ts = Mock()
        mock_ts.fetch.return_value = mock_df
        mock_hourly_fn.return_value = mock_ts

        result = data_acquisition._process_station(("TEST01", Mock()), "TS", start, end)

    assert result is None


def test_process_station_underreported_days():
    """
    Test that _process_station returns None for stations with >50% underreported days.
    """
    dates = []
    pressures = []

    for day in range(7):
        day_dates = pd.date_range(f"2020-01-{day + 1:02d}", periods=3, freq="h")
        dates.extend(day_dates)
        pressures.extend([1013.0] * 3)

    for day in range(7, 10):
        day_dates = pd.date_range(f"2020-01-{day + 1:02d}", periods=24, freq="h")
        dates.extend(day_dates)
        pressures.extend([1013.0] * 24)

    mock_df = pd.DataFrame({"pres": pressures}, index=dates)

    start = datetime(2020, 1, 1)
    end = datetime(2020, 1, 10)

    with patch("migraine_weather.data_acquisition.meteostat.hourly") as mock_hourly_fn:
        mock_ts = Mock()
        mock_ts.fetch.return_value = mock_df
        mock_hourly_fn.return_value = mock_ts

        result = data_acquisition._process_station(("TEST02", Mock()), "TS", start, end)

    assert result is None


def test_make_dataset_filters_invalid_stations():
    """
    Test that make_dataset excludes stations where _process_station returns None.
    """
    station_data = pd.DataFrame(
        {"name": ["Station1", "Station2"], "latitude": [0.0, 1.0], "longitude": [0.0, 1.0]},
        index=["ST001", "ST002"],
    )

    daily_df = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=5),
            "pres_min": [1010.0] * 5,
            "pres_max": [1015.0] * 5,
        }
    )

    def mock_process(args, country_code, start, end):
        station_id, _ = args
        return ("ST001", daily_df) if station_id == "ST001" else None

    with patch("migraine_weather.data_acquisition._process_station", side_effect=mock_process):
        result = data_acquisition.make_dataset(
            "TS", station_data, datetime(2020, 1, 1), datetime(2020, 1, 5)
        )

    assert "ST002" not in result
    assert "ST001" in result


def test_make_dataset_success():
    """
    Test that make_dataset returns a dict of station_id -> daily DataFrame.
    """
    station_data = pd.DataFrame(
        {"name": ["Station1"], "latitude": [0.0], "longitude": [0.0]}, index=["ST001"]
    )

    daily_df = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=365),
            "pres_min": [1010.0] * 365,
            "pres_max": [1015.0] * 365,
        }
    )

    with patch(
        "migraine_weather.data_acquisition._process_station", return_value=("ST001", daily_df)
    ):
        result = data_acquisition.make_dataset(
            "TS", station_data, datetime(2020, 1, 1), datetime(2020, 12, 31)
        )

    assert "ST001" in result
    assert isinstance(result["ST001"], pd.DataFrame)
    assert "pres_min" in result["ST001"].columns
    assert "pres_max" in result["ST001"].columns
    assert "date" in result["ST001"].columns
