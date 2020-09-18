# run via cron at 00:10 local time [los angeles] every morning

import os
import time
import pickle
import shutil
import pandas as pd


# file identifier for composite dataframe
date_id = time.strftime('%Y%m%d')


# convert search traffic data to integer type in order to:
# 1 / merge half-hourly dataframes while retaining most recent cumulative search numbers
# 2 / sort by descending search totals
def traffic_to_int(df):
    int_traffic = []

    for t in df['traffic']:
        if 'K' in t:
            int_traffic.append(int(t.replace('+', '').replace('K', '000')))
        elif 'M' in t:
            int_traffic.append(int(t.replace('+', '').replace('M', '000000')))
        else:
            int_traffic.append(int(t.replace('+', '')))

    df['int_traffic'] = int_traffic
    return df.sort_values('int_traffic', ascending=False).reset_index(drop=True)


# avoid duplicate results due to capitalization, spacing + punctuation discrepancies
punct = ['.', ',', '?', '!', ';', ':', '-', '(', ')', '[', ']', '{', '}', '"', "'"]

def raw_queries(df):
    raw_queries = []

    for i in df['queries']:
        for p in punct:
            if p in i:
                i = i.replace(p, '')
        raw_queries.append(i.lower().replace(' ', ''))

    df['raw_queries'] = raw_queries
    return df


# convert final search traffic data to string format for email display
# [uniform for all data sources]
def format_results(df):
    traffic = df['int_traffic'].map(str)
    formatted_traffic = []

    for i in traffic:
        length = len(i)

        # 10K
        if length == 5:
            if i[2] != '0':
                formatted_traffic.append(f'{i[:2]}.{i[2]}K+')
            else:
                formatted_traffic.append(f'{i[:2]}K+')
        # 100K
        elif length == 6:
            if i[3] != '0':
                formatted_traffic.append(f'{i[:3]}.{i[3]}K+')
            else:
                formatted_traffic.append(f'{i[:3]}K+')
        # 1M
        elif length == 7:
            if i[1] and i[2] == '0':
                formatted_traffic.append(f'{i[0]}M+')
            elif i[2] == '0':
                formatted_traffic.append(f'{i[0]}.{i[1]}M+')
            else:
                formatted_traffic.append(f'{i[0]}.{i[1:3]}M+')
        # 10M
        elif length == 8:
            if i[2] and i[3] == '0':
                formatted_traffic.append(f'{i[:2]}M+')
            elif i[3] == '0':
                formatted_traffic.append(f'{i[:2]}.{i[2]}M+')
            else:
                formatted_traffic.append(f'{i[:2]}.{i[2:4]}M+')
        # 100M
        elif length == 9:
            if i[3] and i[4] == '0':
                formatted_traffic.append(f'{i[:3]}M+')
            elif i[4] == '0':
                formatted_traffic.append(f'{i[:3]}.{i[3]}M+')
            else:
                formatted_traffic.append(f'{i[:3]}.{i[3:5]}M+')
        # 1B [just in case]
        elif length == 10:
            if i[1] and i[2] == '0':
                formatted_traffic.append(f'{i[0]}B+')
            elif i[2] == '0':
                formatted_traffic.append(f'{i[0]}.{i[1]}B+')
            else:
                formatted_traffic.append(f'{i[0]}.{i[1:3]}B+')

        else:
            formatted_traffic.append(i)

    df['formatted_traffic'] = formatted_traffic
    return df


# concatenate, sort, format + save half-hourly dataframes as composite for previous date
def compile():
    files = [f'./google_files/{i}' for i in os.listdir('./google_files') if i.startswith('g_')]

    all_dfs = [pickle.load(open(f, 'rb')) for f in files]
    all_dfs = [traffic_to_int(df) for df in all_dfs]
    df = pd.concat(all_dfs)

    df = raw_queries(df)
    df = df.groupby('raw_queries').agg(max)
    df = df.sort_values('int_traffic', ascending=False).reset_index()
    df = format_results(df)

    pickle.dump(df, open(f'./g_{date_id}.pkl', 'wb'))

    # archive half-hourly dataframes from previous date
    if 'archive' not in os.listdir('./google_files'):
        os.mkdir('./google_files/archive')

    [shutil.move(f'./google_files/{i}', f'./google_files/archive/{i}') for i in os.listdir('./google_files') if i.startswith('g_')]


compile()
