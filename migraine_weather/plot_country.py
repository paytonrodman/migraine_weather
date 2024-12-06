import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import pycountry

cc = 'Australia'
country_name = pycountry.countries.search_fuzzy(cc)[0].name
country_code = pycountry.countries.search_fuzzy(cc)[0].alpha_2

file_path = "/Users/paytonrodman/projects/migraine_pressure/data/processed/"
stations = pd.read_pickle(file_path + f'{country_code}.pkl')
print(stations)

# initialize an axis
fig, ax = plt.subplots(figsize=(12,12))
country_map = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
country_map[country_map["name"] == country_name].plot(color="lightgrey", ax=ax)

stations.plot(x="longitude", y="latitude", kind="scatter", c="frac_var", colormap="YlOrRd", ax=ax)
plt.show()
