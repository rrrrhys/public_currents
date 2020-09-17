# run via cron every 30 minutes

import os
import time
import json
import requests
import pickle
import pandas as pd


# file identifiers for half-hourly dataframes
date_id = time.strftime('%Y%m%d')
time_id = time.strftime('%H%M%S')


# request, parse, format + save trending json data as dataframe
def google_data():
    url = 'https://trends.google.com/trends/api/dailytrends'

    params = {
        'hl': 'en-US',
        'tz': 'Western/US',
        'geo': 'US',
        'ns': 15
    }

    res = requests.get(url, params=params)

    # request + parse
    json_data = json.loads(res.text[5:])
    data = json_data['default']['trendingSearchesDays'][0]

    queries = [i['title']['query'] for i in data['trendingSearches']]
    traffic = [i['formattedTraffic'] for i in data['trendingSearches']]
    links = [f"https://www.google.com/search?q={i.replace(' ', '+')}&tbm=nws" for i in queries]

    # set up dataframe
    columns = ['queries', 'traffic', 'links']
    data_matrix = [[queries[i], traffic[i], links[i]] for i in range(len(queries))]

    df = pd.DataFrame(data_matrix, columns=columns)
    pickle.dump(df, open(f'./google_files/g_{date_id}{time_id}.pkl', 'wb'))


google_data()
