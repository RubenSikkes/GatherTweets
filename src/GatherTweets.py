import tweepy
from datetime import datetime, timedelta
import time
import json

def set_tweepy_auth(file_location):
    # To authenticate tweepy
    with open(file_location, 'r') as f:
        data = json.load(f)
    auth = tweepy.OAuthHandler(data['consumer_key'], data['consumer_secret'])
    auth.set_access_token(data['access_token'], data['access_token_secret'])
    return tweepy.API(auth , wait_on_rate_limit=True)


def del_profile_nonsense(dic):
    deleteKeys = [key for key in dic.keys() if "profile" in key]
    for key in deleteKeys:
        if key in dic:
            del dic[key]
    return dic

class tweepy_connector(object):
    """
    We create a class since this makes it easy to create multiple search objects if we need them.
    They will all contain the basic functions and information we add below.

    """
    def __init__(self, days, keyword, priv_file_loc):
        self.start_time = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        self.until_time = datetime.now().strftime('%Y-%m-%d')
        self.timestamp_now = int(time.time())
        self.api_object = set_tweepy_auth(priv_file_loc)
        self.keyword = keyword

    def simple_search(self):
        results = self.api_object.search(q=f'{self.keyword}', lang='nl', rrp=100, show_user=False)
        return results

    def advanced_search(self):
        query = f'{self.keyword} since:{self.start_time} until:{self.until_time}'
        self.iterations = 0
        self.tweets = []
        for tweet in tweepy.Cursor(self.api_object.search,
                                   q=query,
                                   rpp=100,
                                   include_entities=True,
                                   lang='nl').items():
            if self.iterations % 100 == 0:
                print(f'iteration {self.iterations}!')
            self.tweets.append({'created_at': tweet.created_at,
                           'text': tweet.text,
                           'user': tweet.user._json,
                           'id': tweet.id})
            self.iterations += 1

    def write_results_to_json(self):
        for tweet in self.tweets:
            tweet['created_at'] = tweet['created_at'].strftime("%m-%d-%Y, %H:%M:%S")
            tweet['user'] = del_profile_nonsense(tweet['user'])
            tweet_id = tweet['id']
            with open(f'data/raw/id{tweet_id}_ts{self.timestamp_now}.json', 'w') as outfile:
                json.dump(tweet, outfile)


if __name__=="__main__":
    # This is where we store our credentials
    twit_priv_file_loc = 'data/private/private_keys.json'

    # This is where we create a tweepy object
    Tweepy = tweepy_connector(days=1, keyword='dwdd', priv_file_loc=twit_priv_file_loc)

    # Only a first page search to give an idea of whether this is working
    results = Tweepy.simple_search()
    print(f'Total Results: {len(results)}')

    # Capturing results from multiple pages
    Tweepy.advanced_search()
    print("len advanced search is ", len(Tweepy.tweets))

    # Storing our files
    print("Storing files as JSONs")
    Tweepy.write_results_to_json()