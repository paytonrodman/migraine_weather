# custom_funcs.py

def select_on_hours(df, start, end):
    """
    Processes a dataframe df and returns only those entries with an hourly starttime and endtime
    that encapsulates the range defined by start and end.

    :param pd.DataFrame df: A pandas dataframe. Should contain the following columns:
        - hourly_start
        - hourly_end
    :returns: A modified dataframe.
    """
    return (df[(df['hourly_start'].dt.year<=start.year) & (df['hourly_end'].dt.year>=end.year)])
