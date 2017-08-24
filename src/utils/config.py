import configparser


def _get_twitter_tokens():
    parser = configparser.ConfigParser()
    parser.read('./creds.conf')

    return (
        parser['Twitter']['consumer_key'],
        parser['Twitter']['consumer_secret'],
        parser['Twitter']['access_token'],
        parser['Twitter']['access_token_secret']
    )