"""
Functions for processing data
"""

from typing import List

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
