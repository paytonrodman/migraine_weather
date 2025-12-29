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
├── data
│   └── processed      <- The final list of stations, per country, for plotting
│
├── notebooks          <- Jupyter notebooks.
│
├── pyproject.toml     <- Project configuration file with package metadata
│
├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures        <- Generated graphics and figures to be used in reporting
│
└── migraine_weather   <- Source code for use in this project.
    │
    ├── __init__.py             <- Makes migraine_weather a Python module
    │
    ├── config.py               <- Store useful variables and configuration
    │
    ├── dataset.py              <- Scripts to download or generate data
    │
    ├── plots.py                <- Code to create visualizations
    │
    ├── make_maps.py            <- Useful functions to generate maps
    │
    ├── processing.py           <- Useful functions to generate cleaned data
    │
    └── test                    
        └── test_processing.py  <- Testing script for functions in processing.py
```

--------
