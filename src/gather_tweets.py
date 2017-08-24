import tweepy
import logging
from logging import handlers
import re
import pickle
from datetime import date

from utils import config

logging.basicConfig()
logger = logging.getLogger('tweepy')
logger.addHandler(handlers.RotatingFileHandler('./logs/tweepy.log', maxBytes=1e6, backupCount=5))
logger.setLevel('DEBUG')

class TwitterClient(object):

    def __init__(self):
        self.consumer, self.consumer_secret, self.access, self.access_secret = config._get_twitter_tokens()
        self._logger = logger
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

    def _write_to_file(self, tweets, filename='./data/tweets.pkl'):
        with open(filename, 'wb') as fd:
            pickle.dump(tweets, fd)
            self._logger.info('Successfully wrote tweets to file')

    def get_tweets(self, query="", count=15):
        '''
                Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = []

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self._api.search(q=query, count=count)

            # parsing tweets one by one
            for tweet in fetched_tweets:
                # empty dictionary to store required params of a tweet
                parsed_tweet = self.clean_tweet(tweet.text)

                # appending parsed tweet to tweets list
                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet not in tweets:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)

            # return parsed tweets
            self._write_to_file(tweets, './data/{date}.pkl'.format(date=date.today()))

        except tweepy.TweepError as e:
            # print error (if any)
            self._logger.error("Error : " + str(e))

if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--count', default=25)
    argparser.add_argument('--query', default='test', nargs='?', type=str)
    args = argparser.parse_args()

    api = TwitterClient()
    api.get_tweets(args.query, args.count)




