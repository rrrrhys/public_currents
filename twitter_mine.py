# run via cron every 15 minutes

import os
import time
import json
import requests
import random
import pickle
import subprocess
import pandas as pd


# file identifiers for quarter-hourly dataframes
date_id = time.strftime('%Y%m%d')
time_id = time.strftime('%H%M%S')


# generate guest token using random common user agent to access trending topic api
def generate_token():
    common_user_agents = json.load(open('./common_user_agents.json'))
    weights = [float(i['percent'].replace('%', '')) for i in common_user_agents]
    user_agents = [i['useragent'] for i in common_user_agents]

    random_user_agent = random.choices(user_agents, weights=weights)
    user_agent = f'User-Agent: {random_user_agent}'
    guest_token = subprocess.check_output("curl -skL https://twitter.com/ -H {user_agents[1]} --compressed | grep -o 'gt=[0-9]*' | sed s.gt=..", shell=True, text=True).strip()
    return guest_token


# request, parse, format + save trending json data as dataframe
def twitter_data():
    url = 'https://api.twitter.com/2/guide.json'

    params = {
        'include_profile_interstitial_type': 1,
        'include_blocking': 1,
        'include_blocked_by': 1,
        'include_followed_by': 1,
        'include_want_retweets': 1,
        'include_mute_edge': 1,
        'include_can_dm': 1,
        'include_can_media_tag': 1,
        'skip_status': 1,
        'cards_platform': 'Web-12',
        'include_cards': 1,
        'include_ext_alt_text': 'true',
        'include_quote_count': 'true',
        'include_reply_count': 1,
        'tweet_mode': 'extended',
        'include_entities': 'true',
        'include_user_entities': 'true',
        'include_ext_media_color': 'true',
        'include_ext_media_availability': 'true',
        'send_error_codes': 'true',
        'simple_quoted_tweet': 'true',
        'include_page_configuration': 'true',
        'initial_tab_id': 'trending',
        'entity_tokens': 'false',
        'count': 20,
        'ext': 'mediaStats,highlightedLabel'
    }

    headers = {
        'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
        'x-guest-token': generate_token()
    }

    res = requests.get(url, params=params, headers=headers)

    # request + parse
    json_data = res.json()
    data = json_data['timeline']['instructions'][1]['addEntries']['entries'][1]['content']['timelineModule']['items']

    subjects = [i['item']['content']['trend']['name'] for i in data if 'metaDescription' in i['item']['content']['trend']['trendMetadata'].keys()]
    tweets = [i['item']['content']['trend']['trendMetadata']['metaDescription'].replace(' Tweets', '') for i in data if 'metaDescription' in i['item']['content']['trend']['trendMetadata'].keys()]

    # ensure links are viable for both hashtags + phrases
    links = []
    for i in subjects:
        if i.startswith('#'):
            links.append(f"https://twitter.com/search?q={i.replace('#', '%23', 1).replace(' ', '%20')}")
        else:
            links.append(f"https://twitter.com/search?q={i.replace(' ', '%20')}")

    # set up dataframe
    columns = ['subjects', 'tweets', 'links']
    data_matrix = [[subjects[i], tweets[i], links[i]] for i in range(len(subjects))]

    df = pd.DataFrame(data_matrix, columns=columns)
    pickle.dump(df, open(f'./twitter_files/t_{date_id}{time_id}.pkl', 'wb'))


twitter_data()
