
import logging
from pathlib import Path

from .consts import FIGURES_DIR, PROCESSED_DATA_DIR, LONG_LAT_DICT
from migraine_weather import make_maps


def plots(
    input_path: Path = PROCESSED_DATA_DIR,
    output_path: Path = FIGURES_DIR,
):
    """
    Generates plots for predefined regions of the world map

    Args:
        Path input_path: The location of the processed data for plotting.
        Path output_path: The location to save resulting figures

    Returns:
        None
    """
    for region in LONG_LAT_DICT.keys():
        logging.info("Generating plot from data for %s...", region)
        make_maps.plot_region(region, input_path, output_path)

    logging.info("Plot generation complete.")
