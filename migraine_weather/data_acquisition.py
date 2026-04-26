"""
Functions for processing data
"""

import logging
import warnings

from typing import List
from datetime import datetime

import meteostat
import pandas as pd

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

from . import processing

pd.set_option("mode.copy_on_write", True)


def _process_station(args):
    """Worker function to process a single station."""
    station, station_df, country_code = args

    # Re-index to date only
    station_df = station_df.reset_index(["station"])

    # Calculate completeness
    na_mask = station_df["pres"].isna()
    completeness = 1 - na_mask.sum() / len(na_mask)
    day_complete = (
        station_df.groupby(pd.Grouper(freq="D")).count()["pres"].value_counts(normalize=True)
    )
    underreported_days = sum(day_complete[day_complete.index < 6])

    if completeness < 0.5 or underreported_days > 0.5:
        return float("nan")

    # Remove days with outliers from dataset
    cleaned_df = processing.remove_outliers(station_df)
    return processing.get_variation_frac(cleaned_df)


def get_eligible_stations(freq: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    A function to determine if a file exists for a given country, or whether
    it should be overwritten.

    Args:
        string freq: A string corresponding to data frequency neeed (e.g. daily, hourly).
        datetime start: A datetime object. The start datetime for data analysis
        datetime end: A datetime object. The end datetime for data analysis

    Returns:
        pd.DataFrame eligible_stations: A pandas dataframe object with all
            eligible stations.
    """
    eligible_stations: pd.DataFrame = (
        meteostat.Stations().inventory(freq.lower(), (start, end)).fetch()
    )  # get all stations with right hourly data, worldwide
    return eligible_stations


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
    av_frac_var: List[float] = []

    n_stations: int = len(country_station_data)
    if not n_stations:
        logging.warning("No suitable stations available for country code %s.", country_code)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        hourly_data: pd.DataFrame = meteostat.Hourly(
            country_station_data, start, end, model=False
        ).fetch()

    # Prepare args for parallel processing
    station_args = [
        (station, station_df, country_code)
        for station, station_df in hourly_data.groupby(by="station")
    ]
    # Process in parallel
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        av_frac_var = list(executor.map(_process_station, station_args))

    # Add fractional variation to dataframe and drop NaNs
    country_station_data.insert(0, "frac_var", av_frac_var)
    country_station_data = country_station_data.drop(
        country_station_data[country_station_data["frac_var"].isna()].index
    )
    return country_station_data
