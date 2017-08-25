import tweepy
import logging
import logging.config
import re
import sqlite3
import time
import yaml

from utils import config

with open('./logs.conf', 'r') as fd:
    logging.config.dictConfig(yaml.load(fd.read()))

logger = logging.getLogger('gather_tweets')

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

    def get_tweets(self, query="", table=""):
        '''
                Main function to fetch tweets and parse them.
        '''

        try:
            num_iters = 4
            while num_iters > 0:
                clean_tweets = []
                num_iters -= 1
                num_calls = 0
                while num_calls < 180:
                    num_calls += 1

                    tweets = self._api.search(q=query, count=100)

                    for tweet in tweets:
                        clean = self.clean_tweet(tweet.text)

                        if tweet.retweet_count > 0:
                            # if tweet has retweets, ensure that it is appended only once
                            if clean not in clean_tweets:
                                clean_tweets.append(clean)
                        else:
                            clean_tweets.append(clean)
                self._write_to_db(clean_tweets, table)
                time.sleep(15 * 60)




        except tweepy.TweepError as e:
            # print error (if any)
            self._logger.error("Error : " + str(e))

if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--query', default='test', nargs='?', type=str)
    argparser.add_argument('--table', default='', nargs='?', type=str)
    args = argparser.parse_args()

    api = TwitterClient()
    api.get_tweets(args.query, args.table)




