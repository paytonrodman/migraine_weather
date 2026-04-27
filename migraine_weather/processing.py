"""
Functions for processing data
"""

import pandas as pd
from pandas import DatetimeIndex


def get_daily_pressure_range(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily min/max pressure after removing outliers.

    Returns:
        pd.DataFrame with columns: date, pres_min, pres_max
    """
    cleaned = remove_outliers(dataframe)  # Remove days with outliers from dataset

    daily = cleaned["pres"].groupby(pd.Grouper(freq="D")).agg(["min", "max"])
    daily.columns = ["pres_min", "pres_max"]
    daily.index.name = "date"
    return daily.reset_index()


def remove_outliers(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Processes a dataframe df to remove days with outlier measurements in pressure.

    Args:
        pd.DataFrame dataframe: A pandas dataframe of a single station. Should contain a column 'pres'

    Returns:
        pd.DataFrame cleaned_df: A cleaned pandas dataframe.
    """
    # Calculate pressure variation per hour
    dt = dataframe.index.to_series().diff().dt.total_seconds() / 3600
    dpres = dataframe["pres"].diff() / dt

    # Find outliers with >=2 variations outside 3 IQR
    q25, q75 = dpres.quantile([0.25, 0.75])
    iqr = q75 - q25
    is_outlier: pd.Series = (dpres < (q25 - 3 * iqr)) | (dpres > (q75 + 3 * iqr))
    if not is_outlier.any():
        return dataframe
    outlier_dates = pd.DatetimeIndex(dpres[is_outlier].index).normalize()
    drop_dates = set(outlier_dates.value_counts()[lambda x: x > 1].index)

    # Mask outlier days from dataframe
    mask = DatetimeIndex(dataframe.index).normalize().isin(drop_dates)
    return dataframe[~mask]


def compute_frac_var(daily_df: pd.DataFrame, thresh: float = 10.0) -> float:
    """
    Calculates the number of days with high pressure variation.

    Args:
        pd.DataFrame daily_df: A pandas dataframe of a single station. Should contain a column 'pres'
        thresh: pressure change threshold in hPa

    Returns:
        float fdays_yearly: The fraction of days per year with high pres variation.
    """
    daily_df = daily_df.copy()
    daily_df["high"] = (daily_df["pres_max"] - daily_df["pres_min"]) >= thresh

    # Group by year
    daily_df["year"] = pd.to_datetime(daily_df["date"]).dt.year
    yearly = daily_df.groupby("year")["high"].agg(["sum", "count"])
    yearly = yearly[yearly["count"] > 0]
    if yearly.empty:
        return float("nan")

    return float((yearly["sum"] / yearly["count"]).mean())
