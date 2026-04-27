"""
Main functions for processing weather data
"""

import logging
from pathlib import Path
from datetime import datetime
from functools import partial
import pandas as pd

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import meteostat
from migraine_weather import data_acquisition
from migraine_weather.consts import DATA_DIR
from migraine_weather.utils import get_country_codes

meteostat.config.block_large_requests = False


def process_country(
    country_code: str,
    all_eligible_stations: pd.DataFrame,
    start: datetime,
    end: datetime,
    daily_output_path: Path,
):
    """Process a single country with parallel station processing."""
    country_stations = all_eligible_stations[all_eligible_stations["country"] == country_code]
    if country_stations.empty:
        return

    # Skip if all stations already have data
    missing = [
        s for s in country_stations.index if not (daily_output_path / f"{s}.parquet").exists()
    ]
    if not missing:
        return

    logging.info("Processing %s...", country_code)
    station_daily_data = data_acquisition.make_dataset(country_code, country_stations, start, end)

    for station_id, daily_df in station_daily_data.items():
        daily_df.to_parquet(daily_output_path / f"{station_id}.parquet", index=False)


def main(
    daily_output_path: Path = Path(DATA_DIR.format(project_root=".") + "/daily"),
):
    daily_output_path.mkdir(exist_ok=True)

    # Date range to analyse
    start = datetime(2010, 1, 1, 0, 0, 0)
    end = datetime.now()
    all_eligible_stations = data_acquisition.get_eligible_stations(start, end)

    # Process countries in parallel
    country_codes = get_country_codes()

    process_func = partial(
        process_country,
        all_eligible_stations=all_eligible_stations,
        start=start,
        end=end,
        daily_output_path=daily_output_path,
    )
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        executor.map(process_func, country_codes)

    logging.info("Processing dataset complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
