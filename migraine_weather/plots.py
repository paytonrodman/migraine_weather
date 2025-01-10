from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm

from migraine_weather.config import FIGURES_DIR, PROCESSED_DATA_DIR
from migraine_weather import make_maps
import pycountry

app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = PROCESSED_DATA_DIR,
    output_path: Path = FIGURES_DIR,
    # -----------------------------------------
):


    region_list = ['Africa', 'Asia', 'Europe', 'North America', 'Oceania', 'South America', 'World']
    for region in region_list:
        logger.info(f"Generating plot from data for {region}...")
        make_maps.plot_region(region, input_path, output_path)

    logger.success("Plot generation complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
