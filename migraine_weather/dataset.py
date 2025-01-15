from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm

import glob

from migraine_weather.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from migraine_weather import make_dataset
from datetime import datetime

app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR,
    output_path: Path = PROCESSED_DATA_DIR,
    # ----------------------------------------------
):

    # get list of valid country codes
    country_codes = make_dataset.get_country_codes()
    # get list of country codes from existing datafiles
    data_files = [d.split('/')[-1][:2] for d in glob.glob(str(output_path / '*'))]

    # date range to analyse
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)

    import meteostat
    from meteostat import Stations
    import warnings

    eligible_stations = make_dataset.get_eligible_stations('hourly', start, end)
    for cc, cc_group in eligible_stations: # loop through country groups
        logger.info("Checking if processed data exists...")
        file_exists = make_dataset.check_file_exists(cc, data_files, overwrite_flag=1)

        if not file_exists:
            logger.info(f"Generating dataset for {cc}...")
            data = make_dataset.make_dataset(cc, cc_group, start, end)
            data.to_csv(output_path / f"{cc}.csv")

    logger.success("Processing dataset complete.")

    # -----------------------------------------


if __name__ == "__main__":
    app()
