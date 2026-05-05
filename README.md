# Migraine Weather

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

Analysis of weather data, specifically barometric air pressure changes, for migraine risk assessment.

A short writeup, plus an **interactive plot** is available on my website [here](https://www.paytonelyce.com/project/migraine_pressure/)!

## Project Organization

```
├── LICENSE            <- License for use
├── README.md          <- The top-level README for developers using this project.
├── Makefile           <- Convenience commands (test, coverage)
├── pyproject.toml     <- Project configuration file with package metadata and dependencies
├── uv.lock            <- Locked dependency versions (managed by uv)
├── main.py            <- Entry point for running the full pipeline
│
├── data
│   ├── interim        <- Intermediate per-country station data (one CSV per country code)
│   └── processed      <- Final merged station list used for plotting
│
├── notebooks          <- Jupyter notebooks for exploration and analysis
│
├── figures            <- Generated map figures (per continent + world)
│
├── tests              <- Test suite
│   ├── conftest.py             <- Shared pytest fixtures
│   ├── test_data_acquisition.py
│   ├── test_processing.py
│   └── test_utils.py
│
└── migraine_weather   <- Source code for use in this project.
    │
    ├── __init__.py             <- Makes migraine_weather a Python module
    │
    ├── consts.py               <- Constants and configuration values
    │
    ├── data_acquisition.py     <- Scripts to download and fetch station data
    │
    ├── processing.py           <- Functions to clean and process data
    │
    ├── utils.py                <- General utility/helper functions
    │
    └── visualisation
        └── make_maps.py        <- Functions to generate map visualisations
```

## Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
uv sync
```

## Running

```bash
uv run python main.py
```

## Development

Run tests:
```bash
make test
```

Run tests with coverage report:
```bash
make coverage
```

--------
