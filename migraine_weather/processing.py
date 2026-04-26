"""
Functions for processing data
"""

import logging
import warnings
import glob

from typing import List
from datetime import datetime
from pathlib import Path

import pycountry
import meteostat
import pandas as pd
from pandas import DatetimeIndex

pd.set_option("mode.copy_on_write", True)


def check_file_exists(cc: str, data_files: List[str], overwrite: bool = False) -> bool:
    """
    A function to determine if a file exists for a given country, or whether
    it should be overwritten.

    Args:
        string cc: An ISO 2 country code.
        Iterable data_files: List of data files that already exist.
        bool overwrite: A flag indicating data should be overwritten.

    Returns:
        A boolean indicating the file existence/overwrite state
    """

    return False if ((overwrite) or (cc not in data_files)) else True


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

    # group by station ID
    for station, station_df in hourly_data.groupby(by="station"):
        # reindex to date only
        station_df: pd.DataFrame = station_df.reset_index(["station"])  # type: ignore[no-redef]

        # calculate the total completeness and daily completeness
        na_mask = station_df["pres"].isna()
        completeness = 1 - na_mask.sum() / len(na_mask)
        day_complete: pd.Series[float] = (
            station_df.groupby(pd.Grouper(freq="D")).count()["pres"].value_counts(normalize=True)
        )
        underreported_days: float = sum(day_complete[day_complete.index < 6])

        if completeness < 0.5:
            logging.warning(
                "Completeness below 50 percent for station %s, %s.", station, country_code
            )
            av_frac_var.append(float("nan"))
            continue
        if underreported_days > 0.5:
            logging.warning(
                "More than 50 percent underreported days for station %s, %s.",
                station,
                country_code,
            )
            av_frac_var.append(float("nan"))
            continue

        # Remove days with outliers from dataset
        cleaned_df = remove_outliers(station_df)

        # Calculate fraction of days per year with high pressure variation
        frac_var_yearly = get_variation_frac(cleaned_df)
        av_frac_var.append(frac_var_yearly)

    # Add fractional variation to dataframe and drop NaNs
    country_station_data.insert(0, "frac_var", av_frac_var)
    country_station_data = country_station_data.drop(
        country_station_data[country_station_data["frac_var"].isna()].index
    )
    return country_station_data


def remove_outliers(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Processes a dataframe df to remove days with outlier measurements in pressure.

    Args:
        pd.DataFrame dataframe: A pandas dataframe of a single station. Should contain a column 'pres'

    Returns:
        pd.DataFrame cleaned_df: A cleaned pandas dataframe.
    """
    # calculate pressure variation per hour
    dt = (
        dataframe.index.to_series().diff().dt.days * 24.0
        + dataframe.index.to_series().diff().dt.seconds // 3600
    )
    dpres = dataframe["pres"].diff() / dt

    # determine interquartile ranges
    stats = dpres.describe()
    iqr = stats["75%"] - stats["25%"]
    maxq, minq = stats["75%"] + 3 * iqr, stats["25%"] - 3 * iqr

    # Find outliers with >=2 variations outside 3 IQR
    outlier_dates = DatetimeIndex(dpres.index[(dpres < minq) | (dpres > maxq)]).normalize()
    date_counts = pd.Series(outlier_dates).value_counts()
    drop_dates = set(date_counts[date_counts > 1].index)

    # mask outlier days from dataframe
    mask = DatetimeIndex(dataframe.index).normalize().isin(drop_dates)

    return dataframe[~mask]


def get_variation_frac(dataframe: pd.DataFrame) -> float:
    """
    Calculates the number of days with high pressure variation.

    Args:
        pd.DataFrame dataframe: A pandas dataframe of a single station. Should contain a column 'pres'

    Returns:
        float fdays_yearly: The fraction of days per year with high pres variation.
    """
    thresh = 10.0
    pres = dataframe["pres"]

    # Identify which days have pressure variation > threshold
    daily = pres.groupby(pd.Grouper(freq="D")).agg(["max", "min"])
    daily["high"] = (daily["max"] - daily["min"]) >= thresh

    # Count number of days per year exceeding threshold, dropping empty years
    yearly = daily.groupby(pd.Grouper(freq="YE"))["high"].agg(["sum", "count"])
    yearly = yearly[yearly["count"] > 0]
    if yearly.empty:
        return float("nan")

    # Average number of days per year
    frac_var_yearly = (yearly["sum"] / yearly["count"]).mean()

    return float(frac_var_yearly)


def get_country_codes() -> List[str]:
    """
    Produces a list of valid country codes.

    Args:
        None

    Returns:
        A list of valid ISO 2 country codes.
    """

    country_codes: List[str] = []
    for country in list(pycountry.countries):
        country_codes.append(country.alpha_2)
    return country_codes


def compile_data(input_path: Path, output_path: Path):
    """
    Compiles all CSV file data found at input_path into a single
    CSV file saved to output_path

    Args:
        Path input_path: the Path location to compile
        Path output_path: the Path location to save to

    Returns:
        None
    """

    csv_files = glob.glob(str(input_path / "*.csv"))

    data_list: List[pd.DataFrame] = []
    for file in csv_files:
        data = pd.read_csv(file)
        if not data.empty:
            data_list.append(data)

    compiled_data = pd.concat(data_list)
    compiled_data.set_index("id", inplace=True)
    compiled_data.to_csv(output_path / "all.csv")
