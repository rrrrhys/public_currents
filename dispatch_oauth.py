# run via cron at 02:04 local time [los angeles] every morning

import os
import time
import smtplib
import pickle
import shutil
import base64

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


# file identifier to load dataframes
date_id = time.strftime('%Y%m%d')

# read in list of recipients
recipients = open('./recipients.txt').read().split()

# load + refresh oauth access token
# [unique token acquired via gmail api]
creds = pickle.load(open('./token.pkl', 'rb'))
creds.refresh(Request())

# initiate gmail api
service = build('gmail', 'v1', credentials=creds)


# load dataframes + format relevant data in html, email html to recipients
def email(recipient):

    # keep time consistent regardless of daylight savings
    os.environ['TZ'] = 'EST'
    time.tzset()

    current_time = time.strftime('%H:%M:%S')
    current_date = time.strftime('%m-%d-%Y')

    data = [f'g_{date_id}.pkl', f't_{date_id}.pkl', f'w_{date_id}.pkl']

    g = pickle.load(open(data[0], 'rb'))
    t = pickle.load(open(data[1], 'rb'))
    w = pickle.load(open(data[2], 'rb'))


    email = MIMEMultipart()

    # specify parts of email
    email['subject'] = f'{current_time} {time.tzname[1]}'
    email['from'] = 'PUBLIC CURRENTS <today@publiccurrents.com>'
    email['to'] = recipient

    # html formatted to include top ten results for google / twitter / wikipedia from previous date
    text = f'''
    <body style="background-color: #FFFFF0">
    <font face="Menlo">

    {current_date}<br>
    -<br>
    <br>

    <b>// google [searches]</b><br>
    <br>
    {g.loc[0, 'queries']} [{g.loc[0, 'formatted_traffic']}]<br>
    <a href={g.loc[0, 'links']}>{g.loc[0, 'links']}</a><br>
    <br>
    {g.loc[1, 'queries']} [{g.loc[1, 'formatted_traffic']}]<br>
    <a href={g.loc[1, 'links']}>{g.loc[1, 'links']}</a><br>
    <br>
    {g.loc[2, 'queries']} [{g.loc[2, 'formatted_traffic']}]<br>
    <a href={g.loc[2, 'links']}>{g.loc[2, 'links']}</a><br>
    <br>
    {g.loc[3, 'queries']} [{g.loc[3, 'formatted_traffic']}]<br>
    <a href={g.loc[3, 'links']}>{g.loc[3, 'links']}</a><br>
    <br>
    {g.loc[4, 'queries']} [{g.loc[4, 'formatted_traffic']}]<br>
    <a href={g.loc[4, 'links']}>{g.loc[4, 'links']}</a><br>
    <br>
    {g.loc[5, 'queries']} [{g.loc[5, 'formatted_traffic']}]<br>
    <a href={g.loc[5, 'links']}>{g.loc[5, 'links']}</a><br>
    <br>
    {g.loc[6, 'queries']} [{g.loc[6, 'formatted_traffic']}]<br>
    <a href={g.loc[6, 'links']}>{g.loc[6, 'links']}</a><br>
    <br>
    {g.loc[7, 'queries']} [{g.loc[7, 'formatted_traffic']}]<br>
    <a href={g.loc[7, 'links']}>{g.loc[7, 'links']}</a><br>
    <br>
    {g.loc[8, 'queries']} [{g.loc[8, 'formatted_traffic']}]<br>
    <a href={g.loc[8, 'links']}>{g.loc[8, 'links']}</a><br>
    <br>
    {g.loc[9, 'queries']} [{g.loc[9, 'formatted_traffic']}]<br>
    <a href={g.loc[9, 'links']}>{g.loc[9, 'links']}</a><br>
    <br>
    <br>

    <b>// twitter [tweets]</b><br>
    <br>
    {t.loc[0, 'subjects']} [{t.loc[0, 'formatted_tweets']}]<br>
    <a href={t.loc[0, 'links']}>{t.loc[0, 'links']}</a><br>
    <br>
    {t.loc[1, 'subjects']} [{t.loc[1, 'formatted_tweets']}]<br>
    <a href={t.loc[1, 'links']}>{t.loc[1, 'links']}</a><br>
    <br>
    {t.loc[2, 'subjects']} [{t.loc[2, 'formatted_tweets']}]<br>
    <a href={t.loc[2, 'links']}>{t.loc[2, 'links']}</a><br>
    <br>
    {t.loc[3, 'subjects']} [{t.loc[3, 'formatted_tweets']}]<br>
    <a href={t.loc[3, 'links']}>{t.loc[3, 'links']}</a><br>
    <br>
    {t.loc[4, 'subjects']} [{t.loc[4, 'formatted_tweets']}]<br>
    <a href={t.loc[4, 'links']}>{t.loc[4, 'links']}</a><br>
    <br>
    {t.loc[5, 'subjects']} [{t.loc[5, 'formatted_tweets']}]<br>
    <a href={t.loc[5, 'links']}>{t.loc[5, 'links']}</a><br>
    <br>
    {t.loc[6, 'subjects']} [{t.loc[6, 'formatted_tweets']}]<br>
    <a href={t.loc[6, 'links']}>{t.loc[6, 'links']}</a><br>
    <br>
    {t.loc[7, 'subjects']} [{t.loc[7, 'formatted_tweets']}]<br>
    <a href={t.loc[7, 'links']}>{t.loc[7, 'links']}</a><br>
    <br>
    {t.loc[8, 'subjects']} [{t.loc[8, 'formatted_tweets']}]<br>
    <a href={t.loc[8, 'links']}>{t.loc[8, 'links']}</a><br>
    <br>
    {t.loc[9, 'subjects']} [{t.loc[9, 'formatted_tweets']}]<br>
    <a href={t.loc[9, 'links']}>{t.loc[9, 'links']}</a><br>
    <br>
    <br>

    <b>// wikipedia [pageviews]</b><br>
    <br>
    {w.loc[0, 'pages']} [{w.loc[0, 'formatted_views']}]<br>
    <a href={w.loc[0, 'links']}>{w.loc[0, 'links']}</a><br>
    <br>
    {w.loc[1, 'pages']} [{w.loc[1, 'formatted_views']}]<br>
    <a href={w.loc[1, 'links']}>{w.loc[1, 'links']}</a><br>
    <br>
    {w.loc[2, 'pages']} [{w.loc[2, 'formatted_views']}]<br>
    <a href={w.loc[2, 'links']}>{w.loc[2, 'links']}</a><br>
    <br>
    {w.loc[3, 'pages']} [{w.loc[3, 'formatted_views']}]<br>
    <a href={w.loc[3, 'links']}>{w.loc[3, 'links']}</a><br>
    <br>
    {w.loc[4, 'pages']} [{w.loc[4, 'formatted_views']}]<br>
    <a href={w.loc[4, 'links']}>{w.loc[4, 'links']}</a><br>
    <br>
    {w.loc[5, 'pages']} [{w.loc[5, 'formatted_views']}]<br>
    <a href={w.loc[5, 'links']}>{w.loc[5, 'links']}</a><br>
    <br>
    {w.loc[6, 'pages']} [{w.loc[6, 'formatted_views']}]<br>
    <a href={w.loc[6, 'links']}>{w.loc[6, 'links']}</a><br>
    <br>
    {w.loc[7, 'pages']} [{w.loc[7, 'formatted_views']}]<br>
    <a href={w.loc[7, 'links']}>{w.loc[7, 'links']}</a><br>
    <br>
    {w.loc[8, 'pages']} [{w.loc[8, 'formatted_views']}]<br>
    <a href={w.loc[8, 'links']}>{w.loc[8, 'links']}</a><br>
    <br>
    {w.loc[9, 'pages']} [{w.loc[9, 'formatted_views']}]<br>
    <a href={w.loc[9, 'links']}>{w.loc[9, 'links']}</a><br>
    <br>

    _
    <br>
    <br>
    to unsubscribe please respond to this email with your request
    <br>
    </font>
    </body>
    '''

    # compile MIME parts and format for api call
    msg = MIMEText(text, 'html')
    email.attach(msg)
    raw_email = {'raw': base64.urlsafe_b64encode(email.as_bytes()).decode()}

    # send email
    service.users().messages().send(userId='me', body=raw_email).execute()


# email each recipient individually [testing]
[email(i) for i in recipients];


# archive composite dataframes from previous date
if 'archive' not in os.listdir():
    os.mkdir('archive')

[shutil.move(i, f'./archive/{i}') for i in os.listdir() if i in data];
