import pytest

from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd

from migraine_weather.utils import get_country_codes, check_file_exists, compile_data


@pytest.mark.parametrize(
    "country_code,overwrite_flag,file_exists",
    [("AU", False, True), ("AU", True, False), ("AA", False, False), ("AA", True, False)],
    ids=(
        "no_overwrite_valid_cc",
        "overwrite_valid_cc",
        "no_overwrite_invalid_cc",
        "overwrite_invalid_cc",
    ),
)
def test_check_file_exists(country_code: str, overwrite_flag: bool, file_exists: bool):
    """
    Function to test check_file_exists from migraine_weather.make_dataset
    """
    country_codes = get_country_codes()  # get list of real country codes
    test_data_files = [i + ".csv" for i in country_codes]  # make a mock list of data files
    test_data_files_regex = [
        d.split("/")[-1][:2] for d in test_data_files
    ]  # remove .csv from mock files

    assert (
        check_file_exists(country_code, test_data_files_regex, overwrite=overwrite_flag)
        == file_exists
    )


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


def test_compile_data():
    """
    Function to test compile_data from migraine_weather.processing
    """
    with TemporaryDirectory() as tmpdir:
        input_path = Path(tmpdir) / "input"
        output_path = Path(tmpdir) / "output"
        input_path.mkdir()
        output_path.mkdir()

        df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        df2 = pd.DataFrame({"id": [3, 4], "value": [30, 40]})
        df1.to_csv(input_path / "file1.csv", index=False)
        df2.to_csv(input_path / "file2.csv", index=False)

        compile_data(input_path, output_path)

        result = pd.read_csv(output_path / "all.csv")
        # test that output contains all rows
        assert len(result) == 4
        # test that output is of type pd.DataFrame
        assert isinstance(result, pd.DataFrame)
