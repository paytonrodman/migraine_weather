"""
Useful constants for module
"""

from pathlib import Path
from typing import Dict, List

DATA_DIR: Path = "{project_root}/data"
RAW_DATA_DIR: Path = "{data_dir}/raw"
INTERIM_DATA_DIR: Path = "{data_dir}/interim"
PROCESSED_DATA_DIR: Path = "{data_dir}/processed"
MODELS_DIR: Path = "{project_root}/models"
REPORTS_DIR: Path = "{project_root}/reports"
FIGURES_DIR: Path = "{reports_dir}/figures"
FIG_SAVE_PATH: Path = "{output_path}/{region}.png"

LONG_LAT_DICT: Dict[str, Dict[str, List[int, int]]] = {
    "World": {
        "lat": [-90, 90], 
        "long": [-180, 180]
    },
    'Asia': {
        "lat": [-20, 80],
        "long": [30, 180]
    },
    'North America': {
        "lat": [10, 80,],
        "long": [-180, -40]
    },
    "South America": {
        "lat": [-60, 15],
        "long": [-90, -30]
    },
    'Europe': {
        "lat": [30, 70],
        "long": [-20, 40]
    },
    'Africa': {
        "lat": [-40, 40],
        "long": [-20, 60]
    },
    'Oceania': {
        "lat": [-60, 30],
        "long": [110, 240]
    }
}
