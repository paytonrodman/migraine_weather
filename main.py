"""
Main functions for processing weather data
"""

import logging
from pathlib import Path
from datetime import datetime
from functools import partial
import pandas as pd
import typer
import pycountry
from typing import Optional

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import meteostat
from migraine_weather import data_acquisition
from migraine_weather.consts import DATA_DIR
from migraine_weather.utils import get_country_codes, save_station_metadata

meteostat.config.block_large_requests = False
app = typer.Typer()


def _init_worker():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.getLogger("meteostat").setLevel(logging.WARNING)


def process_country(
    country_code: str,
    all_eligible_stations: pd.DataFrame,
    start: datetime,
    end: datetime,
    daily_output_path: Path,
):
    """Process a single country with parallel station processing."""
    country = pycountry.countries.get(alpha_2=country_code)
    country_name = country.name if country else country_code
    country_stations = all_eligible_stations[all_eligible_stations["country"] == country_code]
    if country_stations.empty:
        logging.debug("No eligible stations for %s (%s), skipping.", country_name, country_code)
        return

    # Skip if all stations already have data
    missing = [
        s for s in country_stations.index if not (daily_output_path / f"{s}.parquet").exists()
    ]
    if not missing:
        logging.debug(
            "All stations for %s (%s) already processed, skipping.", country_name, country_code
        )
        return

    logging.info(
        "Processing %s (%s) (%d/%d stations missing)...",
        country_name,
        country_code,
        len(missing),
        len(country_stations),
    )
    station_daily_data = data_acquisition.make_dataset(country_code, country_stations, start, end)

    for station_id, daily_df in station_daily_data.items():
        daily_df.to_parquet(daily_output_path / f"{station_id}.parquet", index=False)

    logging.info(
        "Saved %d stations for %s (%s).", len(station_daily_data), country_name, country_code
    )


@app.command()
def main(
    daily_output_path: Path = Path(DATA_DIR.format(project_root=".") + "/daily"),
    max_workers: int = max(1, mp.cpu_count() - 2),
    start_date: datetime = datetime(2010, 1, 1),
    end_date: Optional[datetime] = None,
):
    end_date = end_date or datetime.now()
    daily_output_path.mkdir(exist_ok=True)

    logging.info("Fetching eligible stations for %s to %s...", start_date.date(), end_date.date())
    all_eligible_stations = data_acquisition.get_eligible_stations(start_date, end_date)
    logging.info(
        "Found %d eligible stations across %d countries.",
        len(all_eligible_stations),
        all_eligible_stations["country"].nunique(),
    )

    # Process countries in parallel
    country_codes = get_country_codes()

    process_func = partial(
        process_country,
        all_eligible_stations=all_eligible_stations,
        start=start_date,
        end=end_date,
        daily_output_path=daily_output_path,
    )
    with ProcessPoolExecutor(max_workers=max_workers, initializer=_init_worker) as executor:
        futures = executor.map(process_func, country_codes)
        try:
            for _ in futures:
                pass
        except KeyboardInterrupt:
            logging.info("Interrupted, shutting down...")
            executor.shutdown(wait=False, cancel_futures=True)
            return

    logging.info("Processing dataset complete.")
    save_station_metadata(all_eligible_stations, daily_output_path, Path("data/processed"))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.getLogger("meteostat").setLevel(logging.WARNING)
    app()
