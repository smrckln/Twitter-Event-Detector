import tweepy
import logging
import logging.config
import re
import sqlite3
import time
import yaml
import json

from utils import config

with open('./logs.conf', 'r') as fd:
    logging.config.dictConfig(yaml.load(fd.read()))

logger = logging.getLogger('gather_tweets')

conn = sqlite3.connect('./data/twitter.db')

def handle_limit(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.error.TweepError as e:
            logger.error(str(e))
            time.sleep(15 * 60)

class TwitterClient(object):

    def __init__(self):
        self.consumer, self.consumer_secret, self.access, self.access_secret = config._get_twitter_tokens()
        self._logger = logger
        self.conn = sqlite3.connect('./data/twitter.db')
        try:
            # create OAuthHandler object
            self._auth = tweepy.OAuthHandler(self.consumer, self.consumer_secret)
            # set access token and secret
            self._auth.set_access_token(self.access, self.access_secret)
            # create tweepy API object to fetch tweets
            self._api = tweepy.API(self._auth)
        except:
            self._logger.error("Error: Authentication Failed")

    def clean_tweet(self, tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w +:\ / \ / \S +)", " ", tweet).split())

    def _write_to_db(self, tweets, table):
        for tweet in tweets:
            cursor = self.conn.cursor()

            cursor.execute("insert into {table} (tweet) values (?)".format(table=table), (tweet,))
        self.conn.commit()
        self._logger.info("Successfully inserted into database")

    def get_tweets(self, query="", table="", num_tweets=500):
        '''
                Main function to fetch tweets and parse them.
        '''
        tweets = []
        while len(tweets) < num_tweets:
            self._logger.info('Number of tweets so far: {num}'.format(num=len(tweets)))
            for tweet in handle_limit(tweepy.Cursor(self._api.search,
                                                    q=query,
                                                    count=100).items()):
                clean_tweet = self.clean_tweet(tweet.text)

                if tweet.retweet_count > 0:
                    if clean_tweet not in tweets:
                        tweets.append(clean_tweet)
                else:
                    tweets.append(clean_tweet)
        self._write_to_db(tweets, table)


class Streaming(tweepy.StreamListener):

    def __init__(self, table):
        super().__init__()
        self.table = table

    def on_data(self, data):
        cursor = conn.cursor()
        json_data = json.loads(data)

        if not cursor.execute('select count(id) from {table} where [id] = ?'.format(table=self.table), (json_data['id'],)).fetchone()[0]:
            cursor.execute('insert into {table} (id, tweet) values (?, ?)'.format(table=self.table),
                           (json_data['id'], json_data['text'],))
        conn.commit()
        return True

    def on_error(self, status):
        logger.error(status)


if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--query', default='test', nargs='?', type=str)
    argparser.add_argument('--table', default='', nargs='?', type=str)
    args = argparser.parse_args()

    consumer, consumer_secret, access, access_secret = config._get_twitter_tokens()
    auth = tweepy.OAuthHandler(consumer, consumer_secret)
    auth.set_access_token(access, access_secret)

    stream_listener = Streaming(args.table)

    stream = tweepy.Stream(auth, stream_listener)
    stream.filter(track=[args.query])




