import warnings

from migraine_weather.config import PROCESSED_DATA_DIR

import pandas as pd
pd.set_option("mode.copy_on_write", True)
import pycountry
import meteostat
from loguru import logger


def check_file_exists(cc, data_files, overwrite_flag=0):
    """
    A function to determine if a file exists for a given country, or whether
    it should be overwritten.

    Args:
        string cc: An ISO 2 country code.
        list data_files: List of data files that already exist.
        PosixPath output_path: The output folder where data is stored.
        bool overwrite_flag: A flag indicating data should be overwritten.

    Returns:
        A boolean indicating the file existence/overwrite state
    """

    return 0 if ((overwrite_flag) or (cc not in data_files)) else 1

def make_dataset(cc, start, end):
    """
    Generate a cleaned dataset with yearly fractional variation in pressure.

    Args:
        string cc: An ISO 2 country code.
        datetime start: A datetime object. The start datetime for data analysis
        datetime end: A datetime object. The end datetime for data analysis

    Returns:
        pd.DataFrame stations: A pandas DataFrame with added frac_var column
    """

    stations = meteostat.Stations().region(cc).fetch()
    stations = select_on_hours(stations, start, end)

    n_stations = len(stations)
    if n_stations == 0:
        logger.warning(f'No suitable stations available for {cc}.')

    current_n = 0
    av_frac_var = []
    for station_id in stations.index:
        current_n += 1
        station_name = stations[stations.index==station_id]['name'].iloc[0]
        current_string = str((current_n/n_stations)*100)
        print(f'Checking station {station_name}, {cc}')

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            try:
                df = meteostat.Hourly(station_id, start, end, model=False).fetch()
            except RemoteDisconnected:
                logger.warning(f'Disconnected. Retrying download for {station_name}, {cc}.')
                df = meteostat.Hourly(station_id, start, end, model=False).fetch()


        # if all pressure measurements are NaN, skip station
        if (df['pres'].isna().all()):
            logger.warning(f'No pressure data for station {station_name}, {cc}.')
            av_frac_var.append(float('nan'))
            continue

        # if >50% of pressure measurements are NaN, skip station
        completeness = 1 - (sum(df['pres'].isna())/len(df['pres'].isna()))
        if completeness < 0.5:
            logger.warning(f'Completeness below 50% for station {station_name}, {cc}.')
            av_frac_var.append(float('nan'))
            continue

        # if >50% of days have fewer than 6 valid measurements, skip station
        vc = df.groupby(pd.Grouper(freq='D')).count()['pres'].value_counts(normalize=True)
        underreported_days = sum(vc[vc.index <= 5])
        if underreported_days > 0.5:
            logger.warning(f'More than 50% underreported days for station {station_name}, {cc}.')
            av_frac_var.append(float('nan'))
            continue

        # Remove days with outliers from dataset
        cleaned_df = remove_outliers(df)

        # Calculate fraction of days per year with high pressure variation
        frac_var_yearly = get_variation_frac(cleaned_df)
        av_frac_var.append(frac_var_yearly)

    # Add fractional variation to dataframe and drop NaNs
    stations.insert(0, "frac_var", av_frac_var)
    stations = stations.drop(stations[stations['frac_var'].isna()].index)
    return stations

def remove_outliers(df):
    """
    Processes a dataframe df to remove days with outlier measurements in pressure.

    Args:
        pd.DataFrame df: A pandas dataframe. Should contain a column 'pres'

    Returns:
        pd.DataFrame cleaned_df: A cleaned pandas dataframe
    """

    cleaned_df = df.copy()
    df_var = df.copy()

    # calculate pressure variation per hour
    dt = df.index.to_series().diff().dt.days*24. + df.index.to_series().diff().dt.seconds//3600
    df_var[f'dpres_per_hour'] = df[f'pres'].diff() / dt

    # determine IQR for dvar_per_hour
    statistics = df_var[f'dpres_per_hour'].describe()
    q75 = statistics["75%"]
    q25 = statistics["25%"]
    intr_qr = q75 - q25
    maxq = q75 + (3*intr_qr)
    minq = q25 - (3*intr_qr)

    # Find outliers with >=2 variations outside 3 IQR
    outliers = df_var[(df_var[f'dpres_per_hour'] < minq) | (df_var[f'dpres_per_hour'] > maxq)];
    outliers['date'] = outliers.index.date;
    entry_counts = outliers['date'].value_counts();
    valid_dates = entry_counts[entry_counts > 1].index;
    outliers = outliers[outliers['date'].isin(valid_dates)];

    # drop outlier days from dataframe
    drop_dates = list(set(outliers.index.date))
    cleaned_df = cleaned_df[~pd.Series(cleaned_df.index.date).isin(drop_dates).values]

    return cleaned_df

def get_variation_frac(df):
    """
    Calculates the number of days with high pressure variation.

    Args:
        pd.DataFrame df: A pandas dataframe. Should contain a column 'pres'

    Returns:
        float fdays_yearly: The fraction of days per year with high pres variation.
    """

    thresh = 10.

    fdays_yearly = {}
    # loop over years
    for yname, ygroup in df.groupby(pd.Grouper(freq='YE')):

        if len(ygroup['pres'])==0:
            fdays_yearly[yname.year] = float('nan')
            continue

        ndays = 0
        # loop over days
        for dname, dgroup in ygroup.groupby(pd.Grouper(freq='D')):
            vrange = dgroup['pres'].max() - dgroup['pres'].min()
            if vrange >= thresh:
                ndays += 1

        frac_days = ndays/len(list(set(ygroup.index.date)))
        fdays_yearly[yname.year] = frac_days


    # remove any remaining NaN values
    fdays_yearly = {k: fdays_yearly[k] for k in fdays_yearly if not pd.isna(fdays_yearly[k])}
    try:
        frac_var_yearly = sum(fdays_yearly.values())/len(fdays_yearly)
    except ZeroDivisionError:
        frac_var_yearly = float('nan')

    return frac_var_yearly

def select_on_hours(df, start, end):
    """
    Processes a dataframe df and returns only those entries with an hourly start
    time and end time that encapsulates the range defined by start and end.

    Args:
        pd.DataFrame df: A pandas dataframe. Should contain the following columns:
            - hourly_start
            - hourly_end
        datetime start: A datetime object. The start datetime for data analysis
        datetime end: A datetime object. The end datetime for data analysis

    Returns:
        A modified dataframe.
    """
    return (df[(df['hourly_start'].dt.year<=start.year) & (df['hourly_end'].dt.year>=end.year)])

def get_country_codes():
    """
    Produces a list of valid country codes.

    Args:
        None

    Returns:
        A list of valid ISO 2 country codes.
    """

    country_codes = []
    for c in list(pycountry.countries):
        country_codes.append(c.alpha_2)
    return country_codes
