import pandas as pd

def get_variation_frac(df, var):
    if var == "pres":
        thresh = 10.

    fdays_yearly = {}
    # loop over years
    for yname, ygroup in df.groupby(pd.Grouper(freq='YE')):
        if ygroup[var].isna().all():
            fdays_yearly[yname.year] = float('nan')
            continue
        if sum(ygroup[var].isna())>0.1*len(ygroup[var]):
            fdays_yearly[yname.year] = float('nan')
            continue

        ndays = 0
        # loop over days
        for dname, dgroup in ygroup.groupby(pd.Grouper(freq='D')):
            vrange = dgroup[var].max() - dgroup[var].min()
            if vrange >= thresh:
                ndays += 1

        frac_days = ndays/len(list(set(ygroup.index.date)))
        fdays_yearly[yname.year] = frac_days
    return fdays_yearly
