# run via cron at 00:05 local time [los angeles] every morning

import os
import time
import pickle
import shutil
import pandas as pd


# file identifier for composite dataframe
date_id = time.strftime('%Y%m%d')


# convert tweet data to integer type in order to:
# 1 / merge quarter-hourly dataframes while retaining most recent cumulative tweet numbers
# 2 / sort by descending tweet totals
def tweets_to_int(df):
    int_tweets = []

    for t in df['tweets']:
        if 'K' in t:
            x = [i for i, j in enumerate(t) if j == '.' or j == 'K']
            if x == [2, 4]:
                int_tweets.append(int(t.replace('.', '').replace('K', '00')))
            else:
                int_tweets.append(int(t.replace('.', '').replace('K', '000')))
        elif 'M' in t:
            x = [i for i, j in enumerate(t) if j == '.' or j == 'M']
            if x == [1, 4]:
                int_tweets.append(int(t.replace('.', '').replace('M', '0000')))
        else:
            int_tweets.append(int(t.replace(',', '')))

    df['int_tweets'] = int_tweets
    return df.sort_values('int_tweets', ascending=False).reset_index(drop=True)


# convert final tweet data to string format for email display
# [uniform for all data sources]
def format_results(df):
    tweets = df['int_tweets'].map(str)
    formatted_tweets = []

    for i in tweets:
        length = len(i)

        # 10K
        if length == 5:
            if i[2] != '0':
                formatted_tweets.append(f'{i[:2]}.{i[2]}K+')
            else:
                formatted_tweets.append(f'{i[:2]}K+')
        # 100K
        elif length == 6:
            if i[3] != '0':
                formatted_tweets.append(f'{i[:3]}.{i[3]}K+')
            else:
                formatted_tweets.append(f'{i[:3]}K+')
        # 1M
        elif length == 7:
            if i[1] and i[2] == '0':
                formatted_tweets.append(f'{i[0]}M+')
            elif i[2] == '0':
                formatted_tweets.append(f'{i[0]}.{i[1]}M+')
            else:
                formatted_tweets.append(f'{i[0]}.{i[1:3]}M+')
        # 10M
        elif length == 8:
            if i[2] and i[3] == '0':
                formatted_tweets.append(f'{i[:2]}M+')
            elif i[3] == '0':
                formatted_tweets.append(f'{i[:2]}.{i[2]}M+')
            else:
                formatted_tweets.append(f'{i[:2]}.{i[2:4]}M+')
        # 100M
        elif length == 9:
            if i[3] and i[4] == '0':
                formatted_tweets.append(f'{i[:3]}M+')
            elif i[4] == '0':
                formatted_tweets.append(f'{i[:3]}.{i[3]}M+')
            else:
                formatted_tweets.append(f'{i[:3]}.{i[3:5]}M+')
        # 1B [just in case]
        elif length == 10:
            if i[1] and i[2] == '0':
                formatted_tweets.append(f'{i[0]}B+')
            elif i[2] == '0':
                formatted_tweets.append(f'{i[0]}.{i[1]}B+')
            else:
                formatted_tweets.append(f'{i[0]}.{i[1:3]}B+')

        else:
            formatted_tweets.append(i)

    df['formatted_tweets'] = formatted_tweets
    return df


# concatenate, sort, format + save quarter-hourly dataframes as composite for previous date
def compile():
    files = [f'./twitter_files/{i}' for i in os.listdir('./twitter_files') if i.startswith('t_')]

    all_dfs = [pickle.load(open(f, 'rb')) for f in files]
    all_dfs = [tweets_to_int(df) for df in all_dfs]
    df = pd.concat(all_dfs)

    df = df.groupby('subjects').agg(max)
    df = df.sort_values('int_tweets', ascending=False).reset_index()
    df = format_results(df)

    pickle.dump(df, open(f'./t_{date_id}.pkl', 'wb'))

    # archive quarter-hourly dataframes from previous date
    [shutil.move(f'./twitter_files/{i}', f'./twitter_files/archive/{i}') for i in os.listdir('./twitter_files') if i.startswith('t_')]


compile()
