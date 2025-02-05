from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm

import glob

from migraine_weather.config import RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR
from migraine_weather import make_dataset
from datetime import datetime

app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR,
    interim_output_path: Path = INTERIM_DATA_DIR,
    processed_output_path: Path = PROCESSED_DATA_DIR
    # ----------------------------------------------
):

    # get list of valid country codes
    country_codes = make_dataset.get_country_codes()
    # get list of country codes from existing datafiles
    data_files = [d.split('/')[-1][:2] for d in glob.glob(str(interim_output_path / '*'))]

    # date range to analyse
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)

    # get dataframe containing all stations with required Hourly data around the world
    eligible_stations = make_dataset.get_eligible_stations('hourly', start, end)

    # loop through countries, checking if file exists/should be overwritten
    # then make the dataset for that country
    for cc, cc_group in eligible_stations.groupby(by='country'):
        file_exists = make_dataset.check_file_exists(cc, data_files, overwrite_flag=0)

        if not file_exists:
            logger.info(f"Generating dataset for {cc}...")
            data = make_dataset.make_dataset(cc, cc_group, start, end)
            data.to_csv(interim_output_path / f"{cc}.csv")

    # compile country-level data to a single csv file
    make_dataset.compile_data(interim_output_path, processed_output_path)

    logger.success("Processing dataset complete.")

    # -----------------------------------------


if __name__ == "__main__":
    app()
