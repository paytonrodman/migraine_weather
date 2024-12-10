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
    logger.info("Generating plot from data...")

    #make_maps.plot_country('United States of America', input_path, output_path)
    #make_maps.plot_world(input_path, output_path)

    make_maps.plot_region('Africa', input_path, output_path)

    logger.success("Plot generation complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
