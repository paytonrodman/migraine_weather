"""
Initial configuration for module
"""

from pathlib import Path
import logging

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
logging.info(f"PROJ_ROOT path is: {PROJ_ROOT}")
