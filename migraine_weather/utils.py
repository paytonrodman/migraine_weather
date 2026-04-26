import glob
import logging

from pathlib import Path

import pandas as pd
import pycountry


def check_file_exists(cc: str, data_files: list[str], overwrite: bool = False) -> bool:
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
