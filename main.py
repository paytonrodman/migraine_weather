"""
Main functions for processing weather data
"""

import logging
from pathlib import Path
from datetime import datetime
from functools import partial
import pandas as pd

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import meteostat
from migraine_weather import data_acquisition
from migraine_weather.consts import DATA_DIR, RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR
from migraine_weather.utils import compile_data, get_country_codes

meteostat.config.block_large_requests = False


def process_country(
    country_code: str,
    all_eligible_stations: pd.DataFrame,
    start: datetime,
    end: datetime,
    overwrite: bool,
    interim_output_path: Path,
):
    """Process a single country with parallel station processing."""
    country_stations = all_eligible_stations[all_eligible_stations["country"] == country_code]

    # Skip if file exists and not overwriting
    output_file = interim_output_path / f"{country_code}.csv"
    if output_file.exists() and not overwrite:
        return None

    logging.info("Generating dataset for %s...", country_code)
    eligible = data_acquisition.get_eligible_stations(start, end)
    country_stations = eligible[eligible["country"].startswith(country_code)]

    if country_stations.empty:
        return None

    data = data_acquisition.make_dataset(country_code, country_stations, start, end)
    data.to_csv(output_file)
    logging.info("Saved %s", country_code)
    return country_code


def main(
    input_path: Path = Path(RAW_DATA_DIR.format(DATA_DIR)),
    interim_output_path: Path = Path(INTERIM_DATA_DIR.format(DATA_DIR)),
    processed_output_path: Path = Path(PROCESSED_DATA_DIR.format(DATA_DIR)),
    overwrite: bool = False,
):
    # Date range to analyse
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)
    all_eligible_stations = data_acquisition.get_eligible_stations(start, end)

    # Process countries in parallel
    country_codes = get_country_codes()
    process_func = partial(
        process_country,
        all_eligible_stations=all_eligible_stations,
        start=start,
        end=end,
        overwrite=overwrite,
        interim_output_path=interim_output_path,
    )
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        _completed_cc = executor.map(process_func, country_codes)

    # Compile country-level data to a single csv file
    compile_data(interim_output_path, processed_output_path)

    logging.info("Processing dataset complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
