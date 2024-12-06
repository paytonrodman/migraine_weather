import warnings

import pandas as pd
pd.set_option("mode.copy_on_write", True)

import os
os.chdir("/Users/paytonrodman/projects/migraine_pressure")

import meteostat
from datetime import datetime
from migraine_pressure.data import clean_data, calculate_variation
from migraine_pressure.config import PROCESSED_DATA_DIR
from migraine_pressure.custom_funcs import select_on_hours

import pycountry

country_codes = []
for c in list(pycountry.countries):
    country_codes.append(c.alpha_2)

output_dir = data_dir / 'processed'

check = 'A'
country_codes = [idx for idx in country_codes if idx[0].lower() == check.lower()]

for cc in country_codes:
    start = datetime(2000, 1, 1, 0, 0, 0)
    end = datetime(2020, 12, 31, 23, 59, 59)
    stations = meteostat.Stations().region(cc).fetch()
    #stations = stations[(stations['hourly_start'].dt.year<=start.year) & (stations['hourly_end'].dt.year>=end.year)]
    stations = select_on_hours(stations, start, end)

    n_stations = len(stations)
    current_n = 0
    av_frac_var = []
    for station_id in stations.index:
        current_n += 1
        station_name = stations[stations.index==station_id]['name'].iloc[0]
        print(f'\r{cc}: {(current_n/n_stations)*100:.2f}%', end='', flush=True)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            df = meteostat.Hourly(station_id, start, end).fetch()

        # if pressure measurements are NaN, skip
        if (df["pres"].isna().all()) | (len(df[df["pres"].isna()])>0.5*len(df["pres"])):
            av_frac_var.append(float('nan'))
            continue

        cleaned_df = clean_data.remove_outliers(df)
        frac_var_yearly = calculate_variation.get_variation_frac(cleaned_df, "pres")
        av_frac_var.append(sum(frac_var_yearly.values())/len(frac_var_yearly)) # average fraction of days with high variation per year

    stations.insert(0, "frac_var", av_frac_var)
    stations = stations.drop(stations[stations['frac_var'].isna()].index)

    #stations.to_pickle(output_dir+f"{cc}.pkl")
    stations.to_csv(output_dir+f"{cc}.csv")
