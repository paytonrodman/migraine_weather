"""
Common utility functions
"""

import glob
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
    data_list = [pd.read_csv(f) for f in csv_files if pd.read_csv(f, nrows=0).shape[1] > 0]

    if not data_list:
        logging.warning("No data files found to compile")
        return

    compiled_data = pd.concat(data_list)
    compiled_data.set_index("id", inplace=True)
    compiled_data.to_csv(output_path / "all.csv")
