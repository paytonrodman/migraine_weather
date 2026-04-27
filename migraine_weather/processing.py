"""
Functions for processing data
"""

import pandas as pd
from pandas import DatetimeIndex


def get_daily_pressure_range(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily min/max pressure after removing outliers.

    Args:
        dataframe: Hourly pressure data for a single station. Must contain a 'pres' column.

    Returns:
        DataFrame with columns: date, pres_min, pres_max.
    """
    cleaned = remove_outliers(dataframe)  # Remove days with outliers from dataset

    daily = cleaned["pres"].groupby(pd.Grouper(freq="D")).agg(["min", "max"])
    daily.columns = ["pres_min", "pres_max"]
    daily.index.name = "date"
    return daily.reset_index()


def remove_outliers(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Remove days with outlier pressure measurements from a station DataFrame.

    Args:
        dataframe: Hourly pressure data for a single station. Must contain a 'pres' column.

    Returns:
        DataFrame with outlier days removed.
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
    Calculate the mean annual fraction of days with pressure variation above a threshold.

    Args:
        daily_df: Daily pressure data with columns: date, pres_min, pres_max.
        thresh: Pressure change threshold in hPa.

    Returns:
        Mean fraction of high-variation days per year, or nan if no data.
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
