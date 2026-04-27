"""
Common utility functions
"""

import logging

from pathlib import Path

import pandas as pd
import pycountry


def get_country_codes() -> list[str]:
    """
    Produces a list of valid country codes.

    Args:
        None

    Returns:
        A list of valid ISO 2 country codes.
    """

    return [country.alpha_2 for country in pycountry.countries]


def save_station_metadata(stations: pd.DataFrame, daily_path: Path, output_path: Path):
    """
    Saves metadata for stations that have processed daily data.

    Args:
        stations: DataFrame of all eligible stations (from get_eligible_stations).
        daily_path: Path to directory containing per-station Parquet files.
        output_path: Path to save the metadata CSV.

    Returns:
        None
    """
    processed_ids = {f.stem for f in daily_path.glob("*.parquet")}
    if not processed_ids:
        logging.warning("No processed station data found at %s", daily_path)
        return

    metadata = stations[stations.index.isin(processed_ids)]
    metadata.to_csv(output_path / "stations.csv")
    logging.info("Saved metadata for %d stations.", len(metadata))
