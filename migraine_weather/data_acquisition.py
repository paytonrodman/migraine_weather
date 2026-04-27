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


def _process_station(args: tuple[str, pd.DataFrame], country_code: str):
    """Worker function to process a single station."""
    station, station_df = args

    # Re-index to date only
    station_df = station_df.reset_index(["station"])

    # Calculate completeness
    na_mask = station_df["pres"].isna()
    completeness = 1 - na_mask.sum() / len(na_mask)
    day_complete = (
        station_df.groupby(pd.Grouper(freq="D")).count()["pres"].value_counts(normalize=True)
    )
    underreported_days = sum(day_complete[day_complete.index < 6])

    if completeness < 0.5:
        logging.warning("Completeness below 50%% for station %s, %s.", station, country_code)
        return float("nan")
    if underreported_days > 0.5:
        logging.warning(
            "More than 50%% underreported days for station %s, %s.", station, country_code
        )
        return float("nan")

    # Remove days with outliers from dataset
    cleaned_df = processing.remove_outliers(station_df)
    return processing.get_variation_frac(cleaned_df)


@functools.lru_cache(maxsize=1)
def get_eligible_stations(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch all weather stations with data available for the specified frequency and time range.

    Args:
        string freq: A string corresponding to data frequency needed (e.g. daily, hourly).
        datetime start: A datetime object. The start datetime for data analysis
        datetime end: A datetime object. The end datetime for data analysis

    Returns:
        pd.DataFrame eligible_stations: A pandas dataframe object with all
            eligible stations.
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
) -> pd.DataFrame:
    """
    Generate a cleaned dataset with yearly fractional variation in pressure.

    Args:
        string country_code: An ISO 2 country code.
        pd.DataFrame country_data: A pandas dataframe containing eligible station information.
        datetime start: A datetime object. The start datetime for data analysis
        datetime end: A datetime object. The end datetime for data analysis

    Returns:
        pd.DataFrame stations: A pandas DataFrame with added frac_var column
    """

    n_stations: int = len(country_station_data)
    if not n_stations:
        logging.warning("No suitable stations available for country code %s.", country_code)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        hourly_data = meteostat.hourly(country_station_data, start, end).fetch()

    if hourly_data is None or hourly_data.empty:
        return country_station_data.iloc[0:0]  # empty DataFrame with same columns

    station_counts = hourly_data.groupby(level="station")["pres"].count()
    min_required = len(pd.date_range(start, end, freq="h")) * 0.5
    valid_stations = station_counts[station_counts.gt(min_required)].index
    hourly_data = hourly_data[hourly_data.index.get_level_values("station").isin(valid_stations)]

    # Prepare args for parallel processing
    process = partial(_process_station, country_code=country_code)
    station_groups = list(hourly_data.groupby(level="station"))
    with ThreadPoolExecutor(max_workers=4) as executor:
        av_frac_var = list(executor.map(process, station_groups))

    # Add fractional variation to dataframe and drop NaNs
    country_station_data = country_station_data[country_station_data.index.isin(valid_stations)]
    country_station_data.insert(0, "frac_var", av_frac_var)

    country_station_data = country_station_data.drop(
        country_station_data[country_station_data["frac_var"].isna()].index
    )
    return country_station_data
