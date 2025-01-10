import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import pycountry

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams['font.sans-serif'] = ['Open Sans']

def plot_region(region, input_path, output_path):
    """
    A function to plot a particular region of the Earth.

    Args:
        string region: The region to be plotted. Must be selected from
            ['Asia', 'North America', 'South America', 'Oceania', 'Europe', 'Africa', 'World']
        PosixPath input_path: The location of data files.
        PosixPath output_path: The output folder where the plot will be saved.

    Returns:
        None
    """
    import cartopy.crs as ccrs

    region_list = ['Asia', 'North America', 'South America', 'Oceania', 'Europe', 'Africa', 'World']
    if region not in region_list:
        logger.error("Region not found in region list.")

    # get latitude and longitude range for region of interest
    lat_range, lon_range = get_lonlat(region)
    cent_lon = (lon_range[0] + lon_range[1])/2.

    # create world map of all data
    fig = plt.figure(figsize=(12,8))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree(central_longitude=cent_lon))
    im = plot_world(fig, ax, input_path)

    if region != 'World':
        im.set_sizes([5])
    else:
        im.set_sizes([1])

    # restrict plot bounds for region of interest
    ax.set_extent([lon_range[0], lon_range[1], lat_range[0], lat_range[1]], crs=ccrs.PlateCarree())

    cbar = fig.colorbar(im, orientation='vertical', extend='max')
    cbar.set_label('Fraction of days with high pressure variation', rotation=270, labelpad=12)

    plt.savefig(output_path / f'{region}.png', bbox_inches='tight')


def plot_world(fig, ax, input_path):
    """
    A function to plot the entire Earth.

    Args:
        matplotlib Figure object fig: Figure to plot to.
        matplotlib Axes object ax: Axes to plot to.
        PosixPath input_path: The location of data files.

    Returns:
        matplotlib Figure object fig
        matplotlib Axes object ax
    """

    import matplotlib.cm as cm
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    from cartopy.io.img_tiles import GoogleTiles

    # add map features
    ax.add_feature(cfeature.LAND, color='0.9')
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS)

    # get list of all station data available
    appended_stations = get_all_stations(input_path)

    # plot all station data
    sc = plt.scatter(x=appended_stations["longitude"], y=appended_stations["latitude"],
                    c=appended_stations["frac_var"],
                    s=50,
                    vmin=0, vmax=0.3,
                    transform=ccrs.PlateCarree(),
                    cmap=cm.YlOrRd,
                    zorder=10)


    gl = ax.gridlines(draw_labels=True, zorder=0, alpha=0.0)
    gl.top_labels = False
    gl.right_labels = False

    return sc


def get_all_stations(input_path):
    """
    A function to combine all available station data into one dataframe object.

    Args:
        PosixPath input_path: The location of data files.

    Returns:
        pd DataFrame appended_stations: A pandas dataframe object.
    """

    import pycountry
    import os

    country_codes = []
    for c in list(pycountry.countries):
        country_codes.append(c.alpha_2)

    appended_stations = []
    for cc in country_codes:
        fname = input_path / f'{cc}.csv'
        if os.path.isfile(fname):
            stations = pd.read_csv(fname)
            if not stations.empty:
                appended_stations.append(stations)
    appended_stations = pd.concat(appended_stations)

    return appended_stations


def get_lonlat(region):
    """
    A function which returns the latitude and longitude range for a given region.

    Args:
        string region: The region concerned. Must be one of
            ['Asia', 'North America', 'South America', 'Oceania', 'Europe', 'Africa', 'World']

    Returns:
        list lat_range: A list of format [minimum latitude, maximum latitude]
        list lon_range: A list of format [minimum longitude, maximum longitude]
    """
    if region == 'World':
        lat_range = [-90, 90]
        lon_range = [-180, 180]
    elif region == 'Asia':
        lat_range = [-20, 80]
        lon_range = [30, 180]
    elif region == 'North America':
        lat_range = [10, 80]
        lon_range = [-180, -40]
    elif region == 'South America':
        lat_range = [-60, 15]
        lon_range = [-90, -30]
    elif region == 'Europe':
        lat_range = [30, 70]
        lon_range = [-20, 40]
    elif region == 'Africa':
        lat_range = [-40, 40]
        lon_range = [-20, 60]
    elif region == 'Oceania':
        lat_range = [-60, 30]
        lon_range = [110, 240]

    return lat_range, lon_range
