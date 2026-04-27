"""
Functions for processing data
"""

import pandas as pd
from pandas import DatetimeIndex


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
