import json
import boto3
from botocore.vendored import requests
from datetime import datetime, timedelta
import sys
import os
import tweepy

# Consumer API keys
consumer_key = 'LD2kS4YjaEFaiUgp0nATypePz'
consumer_secret = 'E1oGaJEHtQEs9HtgIeTGt7fkqhUJiQyaHEexNjASeb5P6cmeLN'
access_token = '1104839943803805696-nmtlp1VXzdafWNJPCbHjP9xdZeJT3o'
access_token_secret = 'u62uEtUh3Po0tpXVJUmI8pcVcnnrGgKTXxBeNiUcwMd4K'

# Authorizing:
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Formating variables so we can scrape yesterday's data.
yesterday = datetime.now() - timedelta(days=1)
start = yesterday.strftime('%Y-%m-%d')
until = datetime.now().strftime('%Y-%m-%d')

query = "gtst since:{} until:{}".format(start, until)
print("Our Query: ", query)


# Removing a few non interesting variables
def del_profile_nonsense(dic):
    deleteKeys = [key for key in dic.keys() if "profile" in key]
    for key in deleteKeys:
        if key in dic:
            del dic[key]
    return dic


def lambda_handler(event, context):
    try:
        bucket_name = "esg-datahub"
        s3 = boto3.resource("s3")
        # Gather Tweets
        for tweet in tweepy.Cursor(api.search, q=query, rpp=100, include_entities=True, lang='nl').items():
            current_tweet = {'created_at': tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                             # json can't parse datetime objects
                             'text': tweet.text,
                             'user': del_profile_nonsense(tweet.user._json),
                             'id': tweet.id

                             }
            path_name = 'raw/sources/socialmedia_gtst/twitter/{}/{}/{}/'.format(yesterday.year, yesterday.month,
                                                                                yesterday.day)
            object_name = "twitter_{}.json".format(current_tweet['id'])
            s3 = boto3.resource("s3")
            s3_path = path_name + object_name
            s3.Bucket(bucket_name).put_object(Key=s3_path, Body=json.dumps(current_tweet))
        print("Succesful ingest for tweets")
    except:
        print("Unsuccesful, check API Credentials!")