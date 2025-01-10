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

    country_codes = [c for c in country_codes if c[0] in ['A','B','C']]
    #country_codes = [c for c in country_codes if c[0:2] in ['RO','RU','RS','RW','GS','PM']]

    # date range to analyse
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)

    logger.info("Checking if processed data exists...")
    for cc in country_codes:
        file_exists = make_dataset.check_file_exists(cc, data_files, overwrite_flag=1)

        if not file_exists:
            logger.info(f"Generating dataset for {cc}...")
            data = make_dataset.make_dataset(cc, start, end)
            data.to_csv(output_path / f"{cc}.csv")

    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
