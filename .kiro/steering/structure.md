# Project structure

## Source structure

 - `data_acquisition.py` - Initial retrieval and organisation of data
 - `processing.py` - Data cleaning and main analysis
 - `consts.py` - Useful constants
 - `utils.py` - Common utility/helper functions
 - `visualisation/make_maps.py` - Generates maps

## Tests

 - Unit tests are located in `tests/`, following convention `test_<file being tested>.py`
 - Example data (e.g. API responses) is located in `tests/example_data/`
 - Tests are run following instructions in `Makefile`

## Imports

 - Use relative address for imports where possible (e.g. `from . import consts`)
 - Group imports: Common python libraries first, then external modules, then internal modules
