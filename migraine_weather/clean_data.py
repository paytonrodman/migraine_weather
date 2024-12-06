import meteostat
import pandas as pd

def remove_outliers(df):
    # calculate dvar_per_hour
    vars = ["temp","rhum","prcp","wspd","pres"]
    dt = df.index.to_series().diff().dt.days*24. + df.index.to_series().diff().dt.seconds//3600

    cleaned_df = df.copy()
    for var in vars:
        df[f'd{var}_per_hour'] = df[f'{var}'].diff() / dt

        # determine IQR for dvar_per_hour
        statistics = df[f'd{var}_per_hour'].describe()
        q75 = statistics["75%"]
        q25 = statistics["25%"]
        intr_qr = q75 - q25
        maxq = q75 + (3*intr_qr)
        minq = q25 - (3*intr_qr)

        # Find outliers with >=2 variations outside 3 IQR
        outliers = df[(df[f'd{var}_per_hour'] < minq) | (df[f'd{var}_per_hour'] > maxq)];
        outliers['date'] = outliers.index.date;
        entry_counts = outliers['date'].value_counts();
        valid_dates = entry_counts[entry_counts > 1].index;
        outliers = outliers[outliers['date'].isin(valid_dates)];

        # drop outlier days from dataframe
        drop_dates = list(set(outliers.index.date))
        cleaned_df = cleaned_df[~pd.Series(cleaned_df.index.date).isin(drop_dates).values]

    return cleaned_df
