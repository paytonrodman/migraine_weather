# Technology Stack

## Language & Runtime
  - Python 3.12
  - Package management: [uv](https://github.com/astral-sh/uv)

## Core Dependencies
  - **pandas** — data manipulation and analysis
  - **numpy** — numerical operations
  - **meteostat** — weather station data retrieval
  - **pycountry** — country code lookups
  - **pyarrow** — Parquet/Arrow data serialisation

## Visualisation
  - **matplotlib** — plotting
  - **cartopy** — geographic map projections

## Dev Tooling
  - **pytest** + **pytest-cov** — testing and coverage
  - **black** — code formatting (line length: 99)
  - **ruff** — linting and import sorting
  - **flake8** — additional linting
  - **mypy** — static type checking
  - **safety** — dependency vulnerability scanning

## Build
  - Build backend: `flit_core`
  - Common tasks automated via `Makefile`

## Coding Style

- **Type hints**: required on all function signatures (parameters and return types)
- **Docstrings**: Google style; all public functions have a summary line, `Args:`, and `Returns:` sections
- **Logging**: use the `logging` module throughout; never use `print` for runtime output
- **Error handling**: return `None` for expected missing/empty data; raise exceptions for programming errors
- **Linting**: `ruff` is the primary linter and import sorter; `flake8` is secondary — prefer `ruff` when they conflict
