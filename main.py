"""
Main functions for processing weather data
"""

import logging
from pathlib import Path
from datetime import datetime

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

from migraine_weather import data_acquisition
from migraine_weather.consts import DATA_DIR, RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR
from migraine_weather.utils import compile_data, check_file_exists, get_country_codes


def process_country(args):
    """Process a single country with parallel station processing."""
    country_code, start, end, overwrite, existing_files = args

    # Skip if file exists and not overwriting
    if check_file_exists(country_code, existing_files, overwrite):
        logging.info("Skipping %s (file exists)", country_code)
        return None

    logging.info("Generating dataset for %s...", country_code)
    eligible = data_acquisition.get_eligible_stations("hourly", start, end)
    country_stations = eligible[eligible.index.str.startswith(country_code)]

    if country_stations.empty:
        return None

    return country_code, data_acquisition.make_dataset(country_code, country_stations, start, end)


def main(
    input_path: Path = Path(RAW_DATA_DIR.format(DATA_DIR)),
    interim_output_path: Path = Path(INTERIM_DATA_DIR.format(DATA_DIR)),
    processed_output_path: Path = Path(PROCESSED_DATA_DIR.format(DATA_DIR)),
    overwrite: bool = False,
):
    # Get list of existing country files
    existing_files = [f.stem[:2] for f in interim_output_path.glob("*.csv")]

    # Date range to analyse
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)

    # Process countries in parallel
    country_codes = get_country_codes()
    args_list = [(cc, start, end, overwrite, existing_files) for cc in country_codes]
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        results = executor.map(process_country, args_list)

    # Save results
    for result in results:
        if result is not None:
            country_code, data = result
            data.to_csv(interim_output_path / f"{country_code}.csv")

    # Compile country-level data to a single csv file
    compile_data(interim_output_path, processed_output_path)

    logging.info("Processing dataset complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
