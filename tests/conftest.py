import pytest

from datetime import datetime
import meteostat


@pytest.fixture
def test_stations():
    """
    Provides test station data for a single country (Australia, AU).
    """
    au_stations = meteostat.Stations().region("AU").fetch()

    return au_stations


@pytest.fixture
def test_data(test_stations, test_time):
    """
    Provides test weather data for a single station (Canberra).
    """
    start, end = test_time
    station_id = test_stations.loc[test_stations["name"] == "Canberra"].index.item()

    hourly = meteostat.Hourly(station_id, start, end).fetch()

    return hourly


@pytest.fixture
def test_time():
    """
    Provides a test time range.
    """
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)
    return start, end
