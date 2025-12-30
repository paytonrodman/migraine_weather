"""
Functions to make maps of weather data
"""

import logging
from pathlib import Path
from typing import List

import pandas as pd
import matplotlib.axes
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.collections
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.geoaxes import GeoAxes

from .consts import LONG_LAT_DICT, FIG_SAVE_PATH

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = ["Open Sans"]


def plot_region(region: str, input_path: Path, output_path: Path):
    """
    A function to plot a particular region of the Earth.

    Args:
        string region: The region to be plotted. Must be selected from
            ['Asia', 'North America', 'South America', 'Oceania', 'Europe', 'Africa', 'World']
        Path input_path: The location of the concatenated data file.
        Path output_path: The output folder where the plot will be saved.

    Returns:
        None
    """
    if region not in LONG_LAT_DICT.keys():
        logging.error("Region not found in region list.")

    # Get latitude and longitude for the given region
    longitude_range: List = LONG_LAT_DICT[region]["lon"]
    latitude_range: List = LONG_LAT_DICT[region]["lat"]
    cent_lon = (longitude_range[0] + longitude_range[1]) / 2.0

    # create world map of all data
    fig = plt.figure(figsize=(12, 8))
    ax: GeoAxes = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree(central_longitude=cent_lon)) # type: ignore
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


def plot_world(
    ax: GeoAxes, input_path: Path
) -> matplotlib.collections.PathCollection:
    """
    A function to plot the entire Earth.

    Args:
        matplotlib Figure object fig: Figure to plot to.
        matplotlib Axes object ax: Axes to plot to.
        PosixPath input_path: The location of the concatenated data file.

    Returns:
        matplotlib Figure object fig
        matplotlib Axes object ax
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
