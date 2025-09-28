
import logging
from pathlib import Path

from .consts import FIGURES_DIR, PROCESSED_DATA_DIR, LONG_LAT_DICT
from migraine_weather import make_maps


def plots(
    input_path: Path = PROCESSED_DATA_DIR,
    output_path: Path = FIGURES_DIR,
):
    for region in LONG_LAT_DICT.keys():
        logging.info(f"Generating plot from data for {region}...")
        make_maps.plot_region(region, input_path, output_path)

    logging.info("Plot generation complete.")
