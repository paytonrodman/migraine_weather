# Technology Stack

## Language & Runtime
- Python 3.12
- Package management: uv

## Dependencies
- pandas, numpy, meteostat, pycountry, pyarrow
- matplotlib, cartopy
- pytest, black (line length: 99), ruff, flake8, mypy, safety

## Coding Style

- **Type hints**: required on all function signatures (parameters and return types)
- **Docstrings**: Google style; all public functions have a summary line, `Args:`, and `Returns:` sections
- **Logging**: use the `logging` module throughout; never use `print` for runtime output
- **Error handling**: return `None` for expected missing/empty data; raise exceptions for programming errors
- **Linting**: `ruff` is the primary linter and import sorter; `flake8` is secondary — prefer `ruff` when they conflict
