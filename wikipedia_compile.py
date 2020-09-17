# run via cron at 00:25 local time [los angeles] every morning

import os
import bz2
import wget
import time
import pickle
import datetime
import pandas as pd


# file identifier for formatted dataframe
date_id = time.strftime('%Y%m%d')

# date of raw wikipedia data to download / parse
yesterday = str(datetime.date.today() - datetime.timedelta(1))

# list of immaterial pages to exclude from final results
stop_pages = ['Main Page',
              '-',
              'Special:',
              'Portal:',
              'Help:']


# convert final pageview data to string format for email display
# [uniform for all data sources]
def format_results(df):
    views = df['views'].map(str)
    formatted_views = []

    for i in views:
        length = len(i)

        # 10K
        if length == 5:
            if i[2] != '0':
                formatted_views.append(f'{i[:2]}.{i[2]}K+')
            else:
                formatted_views.append(f'{i[:2]}K+')
        # 100K
        elif length == 6:
            if i[3] != '0':
                formatted_views.append(f'{i[:3]}.{i[3]}K+')
            else:
                formatted_views.append(f'{i[:3]}K+')
        # 1M
        elif length == 7:
            if i[1] and i[2] == '0':
                formatted_views.append(f'{i[0]}M+')
            elif i[2] == '0':
                formatted_views.append(f'{i[0]}.{i[1]}M+')
            else:
                formatted_views.append(f'{i[0]}.{i[1:3]}M+')
        # 10M
        elif length == 8:
            if i[2] and i[3] == '0':
                formatted_views.append(f'{i[:2]}M+')
            elif i[3] == '0':
                formatted_views.append(f'{i[:2]}.{i[2]}M+')
            else:
                formatted_views.append(f'{i[:2]}.{i[2:4]}M+')
        # 100M
        elif length == 9:
            if i[3] and i[4] == '0':
                formatted_views.append(f'{i[:3]}M+')
            elif i[4] == '0':
                formatted_views.append(f'{i[:3]}.{i[3]}M+')
            else:
                formatted_views.append(f'{i[:3]}.{i[3:5]}M+')
        # 1B [just in case]
        elif length == 10:
            if i[1] and i[2] == '0':
                formatted_views.append(f'{i[0]}B+')
            elif i[2] == '0':
                formatted_views.append(f'{i[0]}.{i[1]}B+')
            else:
                formatted_views.append(f'{i[0]}.{i[1:3]}B+')

        else:
            formatted_views.append(i)

    df['formatted_views'] = formatted_views
    return df


# download, parse, sort, format + save raw text data file from previous date [UTC] as dataframe
def wiki_data(date):
    year = date[:4]
    month = date[5:7]


    #001 / download, unzip + read in data
    url = f'https://dumps.wikimedia.org/other/pagecounts-ez/merged/{year}/{year}-{month}/pagecounts-{date}.bz2'
    wget.download(url)
    file = bz2.open(f'./pagecounts-{date}.bz2', 'rt', encoding='utf-8', errors='ignore')
    raw_data = file.readlines()


    #002 / parse web + mobile data then combine + format
    data = [i.replace('\n', '').split(' ') for i in raw_data if i.startswith('en.z') or i.startswith('en.m ')]
    pages = [i[1].replace('_', ' ') for i in data]
    views = [int(i[2]) for i in data]
    links = [f'https://en.wikipedia.org/wiki/{i[1]}' for i in data]

    # set up + format dataframe
    columns = ['pages', 'views', 'links']
    data_matrix = [[pages[i], views[i], links[i]] for i in range(len(pages))]

    df = pd.DataFrame(data_matrix, columns=columns)
    df = df.groupby(['pages', 'links']).sum()
    df = df.sort_values('views', ascending=False)[:1000]
    df = format_results(df)

    # remove stop pages
    for i in df.index:
        for sp in stop_pages:
            if i[0].startswith(sp):
                df = df.drop(i)

    df = df.reset_index()


    # 003 / save df + delete raw data file
    pickle.dump(df, open(f'./w_{date_id}.pkl', 'wb'))
    os.remove(f'./pagecounts-{date}.bz2')


wiki_data(yesterday)
