"""
Functions for fetching data
"""

import logging
import warnings
import functools
from functools import partial
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import meteostat
import pandas as pd

from . import processing


def _process_station(
    args: tuple[str, pd.Series],
    country_code: str,
    start: datetime,
    end: datetime,
) -> tuple[str, pd.DataFrame] | None:
    """
    Fetch and process hourly data for a single station.

    Args:
        args: Tuple of (station_id, station_metadata).
        country_code: ISO 2 country code, used for logging.
        start: Start datetime for data fetch.
        end: End datetime for data fetch.

    Returns:
        Tuple of (station_id, daily DataFrame) if station passes quality checks, else None.
    """
    station_id, station_meta = args
    logging.debug("Processing station %s, %s.", station_id, country_code)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        station_df = meteostat.hourly(station_id, start, end).fetch()

    if station_df is None or station_df.empty:
        return None

    # Check completeness
    na_mask = station_df["pres"].isna()
    completeness = 1 - na_mask.sum() / len(na_mask)
    day_complete = (
        station_df.groupby(pd.Grouper(freq="D")).count()["pres"].value_counts(normalize=True)
    )
    underreported_days = sum(day_complete[day_complete.index < 6])

    if completeness < 0.5:
        logging.warning("Completeness below 50%% for station %s, %s.", station_id, country_code)
        return None
    if underreported_days > 0.5:
        logging.warning(
            "More than 50%% underreported days for station %s, %s.", station_id, country_code
        )
        return None

    daily_df = processing.get_daily_pressure_range(station_df)
    return (station_id, daily_df)


@functools.lru_cache(maxsize=1)
def get_eligible_stations(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch all weather stations with pressure data available in the given time range.

    Args:
        start: Start datetime for data availability check.
        end: End datetime for data availability check.

    Returns:
        DataFrame of eligible stations indexed by station id.
    """
    return meteostat.stations.query(
        """
          SELECT DISTINCT s.id, n.name, s.country, s.region,
                 s.latitude, s.longitude, s.elevation, s.timezone
          FROM stations s
          INNER JOIN names n ON s.id = n.station AND n.language = 'en'
          INNER JOIN inventory i ON s.id = i.station
          WHERE i.parameter = 'pres'
            AND i.start <= :end
            AND i.end >= :start
          """,
        index_col="id",
        params={"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")},
    )


def make_dataset(
    country_code: str, country_station_data: pd.DataFrame, start: datetime, end: datetime
) -> dict[str, pd.DataFrame]:
    """
    Fetch and process hourly pressure data per station, returning daily min/max.

    Args:
        country_code: ISO 2 country code.
        country_station_data: DataFrame of eligible stations.
        start: Start datetime for data analysis.
        end: End datetime for data analysis.

    Returns:
        dict mapping station_id -> DataFrame(date, pres_min, pres_max)
    """
    if country_station_data.empty:
        logging.warning("No suitable stations available for country code %s.", country_code)
        return {}

    process = partial(_process_station, country_code=country_code, start=start, end=end)
    station_items = list(country_station_data.iterrows())

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = executor.map(process, station_items)
        try:
            results = list(futures)
        except KeyboardInterrupt:
            logging.info("Interrupted during station processing for %s.", country_code)
            executor.shutdown(wait=False, cancel_futures=True)
            raise

    return {station_id: daily_df for r in results if r is not None for station_id, daily_df in [r]}
