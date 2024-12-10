from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm

import glob

from migraine_weather.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from migraine_weather.data import make_dataset
from migraine_weather.custom_funcs import get_country_codes
from datetime import datetime

app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR,
    output_path: Path = PROCESSED_DATA_DIR,
    # ----------------------------------------------
):

    country_codes = get_country_codes()
    data_files = [d.split('/')[-1][:2] for d in glob.glob(str(output_path / '*'))]

    country_codes = [c for c in country_codes if c[0] in ['E', 'F', 'G']]

    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)

    logger.info("Checking if processed data exists...")
    for cc in country_codes:
        file_exists = make_dataset.check_file_exists(cc, data_files, output_path, overwrite_flag=1)

        if not file_exists:
            logger.info(f"Generating dataset for {cc}...")
            make_dataset.make_dataset(cc, start, end, output_path)

    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
