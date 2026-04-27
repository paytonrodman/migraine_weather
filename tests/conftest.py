import pytest

from datetime import datetime
import meteostat


@pytest.fixture
def test_stations():
    """
    Provides test station data for a single country (Australia, AU).
    """
    au_stations = meteostat.stations.query(
        "SELECT id, name, country, region, latitude, longitude, elevation, timezone "
        "FROM stations INNER JOIN names ON stations.id = names.station AND names.language = 'en' "
        "WHERE country = 'AU'",
        index_col="id",
    )

    return au_stations


@pytest.fixture
def test_data(test_stations):
    """
    Provides test weather data for a single station (Canberra).
    """
    station_id = test_stations.loc[test_stations["name"] == "Canberra"].index.item()
    start = datetime(2020, 1, 1)
    end = datetime(2020, 12, 31, 23, 59, 59)
    return meteostat.hourly(station_id, start, end).fetch()


@pytest.fixture
def test_time():
    """
    Provides a test time range.
    """
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)
    return start, end
