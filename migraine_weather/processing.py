"""
Functions for processing data
"""

import logging
import warnings

import pycountry
import meteostat
import pandas as pd
pd.set_option("mode.copy_on_write", True)


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


def get_eligible_stations(freq, start, end):
    """
    A function to determine if a file exists for a given country, or whether
    it should be overwritten.

    Args:
        string freq: A string corresponding to data frequency neeed (e.g. daily, hourly).
        datetime start: A datetime object. The start datetime for data analysis
        datetime end: A datetime object. The end datetime for data analysis

    Returns:
        pd.DataFrame eligible_stations: A pandas dataframe object with all
        eligible stations.
    """
    eligible_stations = meteostat.Stations().inventory(freq.lower(), (start, end)).fetch() # get all stations with right hourly data, worldwide
    return eligible_stations


def make_dataset(cc, cc_df, start, end):
    """
    Generate a cleaned dataset with yearly fractional variation in pressure.

    Args:
        string cc: An ISO 2 country code.
        pd.DataFrame cc_df: A pandas dataframe containing eligible station information.
        datetime start: A datetime object. The start datetime for data analysis
        datetime end: A datetime object. The end datetime for data analysis

    Returns:
        pd.DataFrame stations: A pandas DataFrame with added frac_var column
    """
    av_frac_var = []

    n_stations = len(cc_df)
    if n_stations == 0:
        logging.warning(f'No suitable stations available for {cc}.')

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        df_country = meteostat.Hourly(cc_df, start, end, model=False).fetch()

    # group by station ID
    for station, station_df in df_country.groupby(by='station'):
        # reindex to date only
        station_df = station_df.reset_index(['station'])

        # calculate the total completeness and daily completeness
        completeness = 1 - (sum(station_df['pres'].isna())/len(station_df['pres'].isna()))
        day_complete = station_df.groupby(pd.Grouper(freq='D')).count()['pres'].value_counts(normalize=True)
        underreported_days = sum(day_complete[day_complete.index < 6])

        if completeness < 0.5:
            logger.warning(f'Completeness below 50% for station {station}, {cc}.')
            av_frac_var.append(float('nan'))
            continue
        elif underreported_days > 0.5:
            logger.warning(f'More than 50% underreported days for station {station}, {cc}.')
            av_frac_var.append(float('nan'))
            continue

        # Remove days with outliers from dataset
        cleaned_df = remove_outliers(station_df)

        # Calculate fraction of days per year with high pressure variation
        frac_var_yearly = get_variation_frac(cleaned_df)
        av_frac_var.append(frac_var_yearly)

    # Add fractional variation to dataframe and drop NaNs
    cc_df.insert(0, "frac_var", av_frac_var)
    cc_df = cc_df.drop(cc_df[cc_df['frac_var'].isna()].index)
    return cc_df


def remove_outliers(df):
    """
    Processes a dataframe df to remove days with outlier measurements in pressure.

    Args:
        pd.DataFrame df: A pandas dataframe of a single station. Should contain a column 'pres'

    Returns:
        pd.DataFrame cleaned_df: A cleaned pandas dataframe.
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
        pd.DataFrame df: A pandas dataframe of a single station. Should contain a column 'pres'

    Returns:
        float fdays_yearly: The fraction of days per year with high pres variation.
    """

    thresh = 10.

    # loop over years
    ndays_yearly = {}
    for yname, ygroup in df.groupby(pd.Grouper(freq='YE')):
        if len(ygroup['pres'])==0:
            ndays_yearly[yname.year] = float('nan')
            continue

        # loop over days
        ndays = 0
        for dname, dgroup in ygroup.groupby(pd.Grouper(freq='D')):
            vrange = dgroup['pres'].max() - dgroup['pres'].min()
            if vrange >= thresh:
                ndays += 1

        frac_days = ndays/len(list(set(ygroup.index.date)))
        ndays_yearly[yname.year] = frac_days

    # remove any remaining NaN values
    ndays_yearly = {k: ndays_yearly[k] for k in ndays_yearly if not pd.isna(ndays_yearly[k])}
    try:
        frac_var_yearly = sum(ndays_yearly.values())/len(ndays_yearly)
    except ZeroDivisionError:
        frac_var_yearly = float('nan')

    return frac_var_yearly

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

def compile_data(input_path, output_path):
    import glob
    csv_files = glob.glob(str(input_path / '*.csv'))

    data_list = []
    for file in csv_files:
        data = pd.read_csv(file)
        if not data.empty:
            data_list.append(data)

    df = pd.concat(data_list)
    df.set_index('id', inplace=True)
    df.to_csv(output_path / "all.csv")
