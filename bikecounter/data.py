import gzip
import os
import re
import requests

from bs4 import BeautifulSoup
import pandas as pd


def get_edinburgh_bike_counter_data(datapath='data',
                                    force_download=False):
    """Download and cache the Edinburgh bike counter dataset

    Data was provided by The City of Edinburgh Council under a UK Open
    Government Licence (OGL). They note that the "dataset includes
    bike counts collected on a hourly-basis between 2007 and 2016,
    from 48 off-road and on-road counters installed in Edinburgh. Data
    sets have 2 or 4 channel counters (due to the width of the road),
    recording the direction of travel, such as north-bound,
    south-bound, east-bound or west-bound. Counts are on an hourly
    basis."

    Parameters
    ----------
    datapath : string (optional)
        location to save the data
    force_download : bool (optional)
        if True, force redownload of data

    Returns
    -------
    data : pandas.DataFrame
        Edinburgh bike counter data

    """
    r = requests.get('https://data.edinburghopendata.info/dataset/bike-counter-data-set-cluster')
    soup = BeautifulSoup(r.content, 'html.parser')

    dfs = []
    for a in soup.find_all('a', attrs={'href': re.compile("\.csv$")}):

        # Download data
        url = a.get('href')
        filename = os.path.basename(url)
        filepath = os.path.join(datapath, filename + '.gz')
        if force_download or not os.path.exists(filepath):
            r = requests.get(url)
            with gzip.open(filepath, 'wb') as f:
                f.write(r.content)

        # Read data and set datetime as index
        df = pd.read_csv(filepath, index_col='date')
        try:  # specify format to speed up datetime parsing
            df.index = pd.to_datetime(df.index, format='%d/%m/%Y')
        except TypeError:  # infer format
            df.index = pd.to_datetime(df.index)
        df.index += pd.to_timedelta(df['time'], unit='h')

        # Only keep the total number of bikes across all channels
        counter = os.path.splitext(filename)[0]
        df = pd.DataFrame(df.filter(regex='channel', axis=1).sum(axis=1),
                          columns=[counter])

        # Remove days on which no bikes were counted
        daily = df.resample('D').sum()
        operating_days = daily.loc[daily[counter] > 0].index
        df = df[df.index.to_series().dt.date.isin(
                    operating_days.to_series().dt.date)]

        dfs.append(df)

    data = pd.concat(dfs, axis=1)
    return data
