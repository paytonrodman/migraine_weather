"""
Functions to make maps of weather data
"""

import logging
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.collections
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.geoaxes import GeoAxes

from ..consts import LONG_LAT_DICT, FIGURES_DIR, FIG_SAVE_PATH, PROCESSED_DATA_DIR

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = ["Open Sans"]


def plots(
    input_path: Path = Path(PROCESSED_DATA_DIR),
    output_path: Path = Path(FIGURES_DIR),
):
    """
    Generate plots for all predefined world regions.

    Args:
        input_path: Location of the processed station data.
        output_path: Directory to save the resulting figures.

    Returns:
        None
    """
    for region in LONG_LAT_DICT.keys():
        logging.info("Generating plot from data for %s...", region)
        plot_region(region, input_path, output_path)

    logging.info("Plot generation complete.")


def plot_region(region: str, input_path: Path, output_path: Path):
    """
    Plot pressure variation data for a specific world region.

    Args:
        region: Region name. Must be one of the keys in LONG_LAT_DICT.
        input_path: Location of the processed station data file.
        output_path: Directory to save the resulting figure.

    Returns:
        None
    """
    if region not in LONG_LAT_DICT.keys():
        logging.error("Region not found in region list.")

    # Get latitude and longitude for the given region
    longitude_range: list = LONG_LAT_DICT[region]["long"]
    latitude_range: list = LONG_LAT_DICT[region]["lat"]
    cent_lon = (longitude_range[0] + longitude_range[1]) / 2.0

    # create world map of all data
    fig = plt.figure(figsize=(12, 8))
    ax: GeoAxes = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree(central_longitude=cent_lon))  # type: ignore
    im = plot_world(ax, input_path)

    if region != "World":
        im.set_sizes([5])
    else:
        im.set_sizes([1])

    # restrict plot bounds for region of interest
    ax.set_extent(
        [longitude_range[0], longitude_range[1], latitude_range[0], latitude_range[1]],
        crs=ccrs.PlateCarree(),
    )

    cbar = fig.colorbar(im, orientation="vertical", extend="max")
    cbar.set_label("Fraction of days with high pressure variation", rotation=270, labelpad=12)

    plt.savefig(FIG_SAVE_PATH.format(output_path, region), bbox_inches="tight")


def plot_world(ax: GeoAxes, input_path: Path) -> matplotlib.collections.PathCollection:
    """
    Plot all station data onto a world map axes.

    Args:
        ax: Cartopy GeoAxes to plot onto.
        input_path: Location of the processed station data file.

    Returns:
        Scatter plot PathCollection for use in a colorbar.
    """
    # add map features
    ax.add_feature(cfeature.LAND, color="0.9")
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS)

    # get list of all station data available
    # appended_stations = get_all_stations(input_path)
    data = pd.read_csv(input_path / "all.csv")

    # plot all station data
    scatter_plot = plt.scatter(
        x=data["longitude"],
        y=data["latitude"],
        c=data["frac_var"],
        s=50,
        vmin=0,
        vmax=0.3,
        transform=ccrs.PlateCarree(),
        cmap=cm.get_cmap("YlOrRd"),
        zorder=10,
    )

    gl = ax.gridlines(draw_labels=True, zorder=0, alpha=0.0)
    gl.top_labels = False
    gl.right_labels = False

    return scatter_plot
