"""
Main functions for processing weather data
"""

import logging
from pathlib import Path
from datetime import datetime
import glob

from migraine_weather.consts import DATA_DIR, RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR
from migraine_weather import processing


def main(
    input_path: Path = Path(RAW_DATA_DIR.format(DATA_DIR)),
    interim_output_path: Path = Path(INTERIM_DATA_DIR.format(DATA_DIR)),
    processed_output_path: Path = Path(PROCESSED_DATA_DIR.format(DATA_DIR)),
):
    # get list of country codes from existing datafiles
    data_files = [d.split("/")[-1][:2] for d in glob.glob(str(interim_output_path / "*"))]

    # date range to analyse
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)

    # get dataframe containing all stations with required Hourly data around the world
    eligible_stations = processing.get_eligible_stations("hourly", start, end)

    # loop through countries, checking if file exists/should be overwritten
    # then make the dataset for that country
    for country_code, country_stations in eligible_stations.groupby(by="country"):
        country_code = str(country_code)
        file_exists = processing.check_file_exists(country_code, data_files, overwrite=False)

        if not file_exists:
            logging.info("Generating dataset for %s...", country_code)
            data = processing.make_dataset(country_code, country_stations, start, end)
            data.to_csv(interim_output_path / f"{country_code}.csv")

    # compile country-level data to a single csv file
    processing.compile_data(interim_output_path, processed_output_path)

    logging.info("Processing dataset complete.")


if __name__ == "__main__":
    main()
