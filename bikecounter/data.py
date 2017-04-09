import os
import re
from urllib.request import urlopen
from urllib.request import urlretrieve

from bs4 import BeautifulSoup
import pandas as pd


def get_edinburgh_bike_counter_data(datapath='data',
                                    force_download=False):
    """Download and cache the Edinburgh bike counter dataset

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
    html_page = urlopen('http://www.edinburghopendata.info/dataset/bike-counter-data-set-cluster')
    soup = BeautifulSoup(html_page, 'html5lib')

    dfs = []
    for a in soup.find_all('a', attrs={'href': re.compile("\.csv$")}):

        # Download data
        url = a.get('href')
        filename = os.path.basename(url)
        filepath = os.path.join(datapath, filename)
        if force_download or not os.path.exists(filepath):
            urlretrieve(url, filepath)

        # Read data and set datetime as index
        df = pd.read_csv(filepath,
                         index_col='date')
        try:
            df.index = pd.to_datetime(df.index, format='%d/%m/%Y')
        except TypeError:
            df.index = pd.to_datetime(df.index)
        df.index = df.index + pd.to_timedelta(df['time'], unit='h')

        # Sum all channels to get single value for each bike counter
        bike_counter_name = os.path.splitext(filename)[0]
        df = pd.DataFrame(df.filter(regex='channel', axis=1).sum(axis=1),
                          columns=[bike_counter_name])
        dfs.append(df)

    data = pd.concat(dfs, axis=1)
    return data
