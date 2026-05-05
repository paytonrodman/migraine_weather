"""
Tests for utils.py
"""

from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd

from migraine_weather.utils import get_country_codes, save_station_metadata


def test_get_country_codes():
    """
    Function to test get_country_codes from migraine_weather.processing
    """
    cc = get_country_codes()
    # test that output is of type list
    assert isinstance(cc, list)
    # test that all entries have 2 letters
    lengths = [len(i) for i in cc]
    assert len(list(set(lengths))) == 1
    assert list(set(lengths))[0] == 2


def test_save_station_metadata():
    """
    Test that save_station_metadata only saves stations with processed Parquet files.
    """
    with TemporaryDirectory() as tmpdir:
        daily_path = Path(tmpdir) / "daily"
        output_path = Path(tmpdir) / "processed"
        daily_path.mkdir()
        output_path.mkdir()

        stations = pd.DataFrame(
            {
                "name": ["A", "B", "C"],
                "country": ["AU", "BE", "CA"],
                "latitude": [0.0, 1.0, 2.0],
                "longitude": [0.0, 1.0, 2.0],
            },
            index=["ST001", "ST002", "ST003"],
        )
        stations.index.name = "id"

        # Only ST001 and ST002 have been processed
        (daily_path / "ST001.parquet").touch()
        (daily_path / "ST002.parquet").touch()

        save_station_metadata(stations, daily_path, output_path)

        result = pd.read_csv(output_path / "stations.csv")
        assert len(result) == 2
        assert "ST003" not in result["id"].values
