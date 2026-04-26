import glob

from typing import List
from pathlib import Path

import pycountry
import pandas as pd


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
