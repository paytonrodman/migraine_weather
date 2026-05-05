"""
Main functions for processing weather data
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta
from functools import partial
import pandas as pd
import typer
import pycountry
from typing import Optional

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import meteostat
from migraine_weather import data_acquisition
from migraine_weather.consts import DATA_DIR, PROCESSED_DATA_DIR
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
    """Process a single country, doing full fetch for new stations and incremental for existing."""
    country = pycountry.countries.get(alpha_2=country_code)
    country_name = country.name if country else country_code
    country_stations = all_eligible_stations[all_eligible_stations["country"] == country_code]
    if country_stations.empty:
        logging.debug("No eligible stations for %s (%s), skipping.", country_name, country_code)
        return

    new_stations = country_stations[
        ~country_stations.index.map(lambda s: (daily_output_path / f"{s}.parquet").exists())
    ]
    existing_stations = country_stations[~country_stations.index.isin(new_stations.index)]
    logging.info(
        "Processing %s (%s): %d new, %d to update...",
        country_name,
        country_code,
        len(new_stations),
        len(existing_stations),
    )

    # Full fetch for new stations
    if not new_stations.empty:
        for station_id, daily_df in data_acquisition.make_dataset(
            country_code, new_stations, start, end
        ).items():
            daily_df.to_parquet(daily_output_path / f"{station_id}.parquet", index=False)

    # Incremental fetch for existing stations
    for station_id in existing_stations.index:
        parquet_file = daily_output_path / f"{station_id}.parquet"
        existing = pd.read_parquet(parquet_file)
        incremental_start = pd.to_datetime(existing["date"].max()).to_pydatetime() + timedelta(
            days=1
        )
        if incremental_start >= end:
            continue
        station_df = existing_stations.loc[[station_id]]
        result = data_acquisition.make_dataset(country_code, station_df, incremental_start, end)
        if station_id in result:
            updated = pd.concat([existing, result[station_id]], ignore_index=True)
            updated.to_parquet(parquet_file, index=False)

    logging.info("Done %s (%s).", country_name, country_code)


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
    save_station_metadata(all_eligible_stations, daily_output_path, PROCESSED_DATA_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.getLogger("meteostat").setLevel(logging.WARNING)
    app()
