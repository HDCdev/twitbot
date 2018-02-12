#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""twitbot ...just another tweeter bot.

Usage:
  twitbot.py [CNF] [options]

Arguments:
  CNF                        config file [default: config.yml]

Options:
  --daemon                   daemonized execution
  --unfollow                 unfollows non followers
  --followers=<screen_name>  followers proccesor (<me> = my account)
  --add2db                   use database
  --getid screen_name        id for a given screen name
  --log=<level>              log level [default: DEBUG]
  --version                  show program's version number and exit
  -h, --help                 show this help message and exit

"""

import logging
import os
import time
from datetime import datetime
from pprint import pformat
from random import randint
from threading import Thread

import pyrebase
import tweepy
import yaml
from docopt import docopt
from tweepy.models import Status
from tweepy.utils import import_simplejson

logger = logging.getLogger('twitbot')
json = import_simplejson()
likes_counter = 0
retweet_counter = 0
utc_date = datetime.utcnow().strftime('%Y%m%d')

VERSION = '0.5'
CONFIG = './config.yml'


class StreamListener(tweepy.StreamListener):
    def __init__(self,
                 api,
                 stream_cnf={},
                 words=None,
                 go_retweet=False,
                 go_follow=False):

        self.me = api.me()
        self.stream_cnf = stream_cnf
        self.filter_params = {
            'me': self.me,
            'words': words,
            'go_retweet': go_retweet,
            'go_follow': go_follow,
        }
        super(StreamListener, self).__init__(api=api)

    def on_status(self, status, **kwargs):
        thread = Thread(
            target=tweet_processor,
            args=(self.api, status,),
            kwargs={**self.filter_params, **kwargs}
        )
        thread.start()

    def on_limit(self, track):
        logger.error('limit reached: %s', str(track))
        time.sleep(60 * params['mins_sleep'])
        return True

    def on_data(self, raw_data):
        data = json.loads(raw_data)

        try:
            screen_name = data['user']['screen_name']
        except KeyError:
            screen_name = None

        if self.me.screen_name == screen_name:
            return True

        try:
            data['mentions'] = [m['id_str']
                                for m in data['entities']['user_mentions']]
        except:
            data['mentions'] = []

        if self.stream_cnf is not None:
            try:
                stream_key = list(self.stream_cnf.keys())[0]
                stream_values = list(self.stream_cnf.values())[0]
            except IndexError:
                stream_key = 'track'
                stream_values = []

        if (stream_key == 'follow' and
                   list(set(data['mentions']).intersection(stream_values))):
            logger.info('mention to target follow detected')
            return True

        try:
            data['tweet_text'] = data['extended_tweet']['full_text']
        except KeyError:
            try:
                data['tweet_text'] = data['text']
            except KeyError:
                data['tweet_text'] = u''

        if 'retweeted_status' in data:
            logger.info('retweeted detected')
            return True
        elif 'in_reply_to_status_id' in data:
            if data['in_reply_to_status_id'] is not None:
                logger.info('reply detected')
                return True
            status = Status.parse(self.api, data)
            if self.on_status(status) is False:
                return False
        elif 'delete' in data:
            return True
        elif 'event' in data:
            status = Status.parse(self.api, data)
            if self.on_event(status) is False:
                return False
        elif 'direct_message' in data:
            status = Status.parse(self.api, data)
            if self.on_direct_message(status) is False:
                return False
        elif 'friends' in data:
            if self.on_friends(data['friends']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(data['limit']['track']) is False:
                return False
        elif 'disconnect' in data:
            if self.on_disconnect(data['disconnect']) is False:
                return False
        elif 'warning' in data:
            if self.on_warning(data['warning']) is False:
                return False
        else:
            logger.error('Unknown message type: %s', str(raw_data))

    def on_error(self, status_code):
        if status_code == 420:
            return False


def get_config(config_file):
    with open(config_file) as stream:
        return yaml.load(stream)


def set_logger(log_level):
    level = logging.getLevelName(log_level.upper())
    fmt = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    logger.setLevel(level)
    handler.setLevel(level)
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    return None

def tweet_processor(api, status, **kwargs):
    global retweet_counter
    global likes_counter
    global utc_date

    try:
        me = kwargs['me']
    except KeyError:
        me = api.me()

    current_utc_date = datetime.utcnow().strftime('%Y%m%d')
    if current_utc_date != utc_date:
        likes_counter = 0
        retweet_counter = 0
        utc_date = current_utc_date
        logger.info('new utc: %s counter initialization!', utc_date)

    try:
        possibly_sensitive = status.possibly_sensitive
    except AttributeError:
        possibly_sensitive = False

    logger.info(
        'processing tweet: %d screen_name: %s location: %s',
        status.id,
        status.user.screen_name,
        status.user.location
    )

    if possibly_sensitive:
        logger.info('sensitive tweet')
        return True

    logger.debug(
        'retweeted: %s (%d) favorited: %s (%d) contains_mentions: %s',
        str(status.retweeted),
        status.retweet_count,
        str(status.favorited),
        status.favorite_count,
        'True' if status.mentions else 'False'
    )

    text = status.tweet_text.splitlines()
    logger.debug('text: %s,', str(text))

    if kwargs['words'] is not None:
        tweet_words = ' '.join(text).split()
        logger.debug('tweet words: %s,', str(tweet_words))

        try:
            look = kwargs['words']['look']
        except KeyError:
            look = None

        try:
            block = kwargs['words']['block']
        except KeyboardInterrupt:
            block = None

        if isinstance(look, list):
            if not any(w.lower() in [tw.lower() for tw in tweet_words]
                       for w in look):
                return True

        if isinstance(block, list):
            if any(w.lower() in [tw.lower() for tw in tweet_words]
                   for w in block):
                logger.info('tweet blocked: %d', status.id)
                return True

    if (kwargs['go_follow'] and
            status.user.followers_count > params['min_followers_count'] and
            status.user.friends_count < params['max_friends_count']):

        friendship = api.show_friendship(source_id=me.id,
                                         target_id=status.user.id)[1]
        if not friendship.following:
            seconds_to_wait = randint(randint(10, 30), 60 * 3)
            logger.info(
                'waiting to follow: %s for %d seconds',
                status.user.screen_name,
                seconds_to_wait
            )
            time.sleep(seconds_to_wait)

            try:
                api.create_friendship(status.user.id)
            except tweepy.TweepError as error:
                logger.error(
                    'unable to follow %s: %s', status.user.screen_name, error
                )
            else:
                logger.info('user: %s followed!', status.user.screen_name)
                if db is not None:
                    user_data = {
                        'screen_name': status.user.screen_name,
                        'id': status.user.id,
                        'location': status.user.location,
                        'followers_count': status.user.followers_count,
                        'friends_count': status.user.friends_count,
                    }

                    db.child('followed').child(
                        status.user.screen_name
                    ).set(user_data, user_db['idToken'])

                logger.info('user stored in firebase')
        else:
            logger.info('%s already followed', status.user.screen_name)

    if retweet_counter < params['max_dairy_retweet'] and (
            kwargs['go_retweet'] or (
                not status.retweeted and (
                    status.retweet_count > params['min_retweet_count'] and
                status.user.followers_count > params['min_followers_count']))):

        seconds_to_wait = randint(randint(10, 30), 60 * 3)
        logger.info(
            'waiting to retweet id: %d for %d seconds',
            status.id,
            seconds_to_wait
        )
        time.sleep(seconds_to_wait)

        try:
            api.retweet(status.id)
        except tweepy.TweepError as error:
            try:
                error_code = error.args[0][0]['code']
            except TypeError:
                rate = api.rate_limit_status()
                logger.error(
                    'unable to retweet: %s, sleeping for %d minutes',
                    error,
                    params['mins_sleep']
                )
                logger.debug('raw limits: %s', pformat(rate))
                time.sleep(60 * params['mins_sleep'])
            else:
                if error_code != 327:
                    logger.error(
                        'unable to retweet %d: %s', status.id, error
                    )
                else:
                    logger.info('already retweeted, id: %d', status.id)
        else:
            logger.info('id: %d retweeted!', status.id)
            retweet_counter += 1

    if likes_counter < params['max_dairy_likes'] and not status.favorited:
        seconds_to_wait = randint(randint(10, 30), 60 * 2)
        logger.info(
            'waiting to favor id: %d for %d seconds',
            status.id,
            seconds_to_wait
        )
        time.sleep(seconds_to_wait)

        try:
            api.create_favorite(status.id)
        except tweepy.TweepError as error:
            try:
                error_code = error.args[0][0]['code']
            except TypeError:
                rate = api.rate_limit_status()
                logger.error(
                    'unable to favorite: %s, sleeping for %d minutes',
                    error,
                    params['mins_sleep']
                )
                logger.debug('raw limits: %s', pformat(rate))
                time.sleep(60 * params['mins_sleep'])
            else:
                if error_code != 139:
                    logger.error(
                        'unable to favor tweet %d: %s', status.id, error
                    )
                else:
                    logger.info('already favorited, id: %d', status.id)
        else:
            logger.info('id: %d favorited!', status.id)
            likes_counter += 1

    return True


def unfollower(api, config_file):
    try:
        omit = [f['user_id'] for f in config_file['omit']]
    except:
        omit = []

    logger.info('white list: %s', str(omit))

    friends_ids = api.friends_ids()

    my_id = api.me().id

    for friend_id in friends_ids:
        friendship = api.show_friendship(
            source_id=my_id,
            target_id=friend_id
        )[1]
        if not friendship.following and friend_id not in omit:
            try:
                api.destroy_friendship(friend_id)
            except tweepy.TweepError:
                pass
            else:
                logger.info('user: %s unfollowed!', friendship.screen_name)

    return None

def followers_processor(api, screen_name=None, max_batch=None):
    if max_batch is None:
        max_batch = params['max_batch']

    batch_count = 0

    if screen_name is None or screen_name == 'me':
        ref_user = api.me()
    else:
        ref_user = get_user(api, screen_name)

    if ref_user is None:
        logger.error('unable to get user for follower processor')
        return None

    logger.info(
        'processing followers for user: %s (%d)',
        ref_user.screen_name,
        ref_user.followers_count
    )

    for follower in tweepy.Cursor(api.followers,
                                  id=ref_user.id).items(max_batch):

        if follower.following:
            logger.info('%s already followed', follower.screen_name)
            continue

        if follower.followers_count < params['min_followers_count']:
            logger.info(
                '%d: not enough followers for %s',
                follower.followers_count,
                follower.screen_name
            )
            continue

        if ((follower.followers_count + params['add_followers_count'] <
             follower.friends_count) and
                follower.followers_count < params['min_followers_extended']):
            logger.info(
                '%d: not enough friends for %s',
                follower.friends_count,
                follower.screen_name
            )
            continue

        batch_count += 1
        logger.info(
            'processing follower: %s batch number: %d',
            follower.screen_name,
            batch_count
        )

        if batch_count % params['step_batch'] == 0:
            seconds_to_wait = 60 * params['mins_sleep'] * 2
            logger.info('batch pause for %d seconds', seconds_to_wait)
            time.sleep(seconds_to_wait)

        try:
            follower.follow()
        except tweepy.TweepError:
            logger.error('unable to follow: %s', follower.screen_name)
            continue

        logger.info('%s followed!', follower.screen_name)

    return None

def get_db():
    config = {
      'apiKey': os.environ['FIRE_KEY'],
      'authDomain': '{0:s}.firebaseapp.com'.format(os.environ['FIRE_ID']),
      'databaseURL': 'https://{0:s}.firebaseio.com'.format(os.environ['FIRE_ID']),
      'projectId': os.environ['FIRE_ID'],
      'storageBucket': '{0:s}.appspot.com'.format(os.environ['FIRE_ID']),
      'messagingSenderId': os.environ['FIRE_SENDER']
    }
    firebase = pyrebase.initialize_app(config)
    auth = firebase.auth()

    user_db = auth.sign_in_with_email_and_password(
        os.environ['FIRE_MAIL'],
        os.environ['FIRE_SECRET']
    )

    return firebase.database(), user_db


def get_api():
    auth = tweepy.OAuthHandler(os.environ['API_KEY'], os.environ['API_SECRET'])
    auth.set_access_token(os.environ['TOKEN'], os.environ['TOKEN_SECRET'])

    return tweepy.API(
        auth,
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True,
        compression=True
    )


def daemon_thread(api, config_file):
    track = config_file['track']
    words = config_file['words']
    follow = [str(f['user_id']) for f in config_file['follow']]

    try:
        languages = config_file['languages']
    except KeyError:
        languages = None

    logger.info('tracking: %s', str(track))
    logger.debug('words: %s', str(words))
    logger.info('follow: %s', str(follow))

    logger.info('stream_tracker launched')
    stream_tracker = tweepy.Stream(
        auth=api.auth,
        listener=StreamListener(
            api,
            stream_cnf={'track': track},
            words=words,
            go_retweet=params['retweet_tracker'],
            go_follow=params['follow_tracker']
        )
    )

    if languages is not None:
        stream_tracker.filter(languages=languages, track=track, async=True)
    else:
        stream_tracker.filter(track=track, async=True)

    logger.info('stream_watcher launched')
    stream_watcher = tweepy.Stream(
        auth=api.auth,
        listener=StreamListener(
            api,
            stream_cnf={'follow': follow},
            words=None,
            go_retweet=params['retweet_watcher'],
            go_follow=params['follow_watcher']
        )
    )

    stream_watcher.filter(follow=follow, async=True)

def get_user(api, screen_name):
    try:
        user = api.get_user(screen_name)
    except tweepy.TweepError as error:
        logger.error('unable to get %s id: %s', screen_name, error)
        return None

    logger.info('user id for %s: %d', screen_name, user.id)

    return user

def main(arguments):
    config = arguments['CNF'] if arguments['CNF'] is not None else CONFIG
    daemon = arguments['--daemon']
    unfollow = arguments['--unfollow']
    add2db = arguments['--add2db']
    followers = arguments['--followers']
    screen_name = arguments['--getid']
    log_level = arguments['--log']

    try:
        config_file = get_config(config)
    except FileNotFoundError:
        print('unable to open file: {0}'.format(config))
        return None

    global params
    params = config_file['params']

    set_logger(log_level)
    api = get_api()

    global db, user_db
    if add2db:
        db, user_db = get_db()
    else:
        db, user_db = None, None

    if screen_name is not None:
        get_user(api, screen_name)

    if unfollow:
        unfollower(api, config_file)

    if daemon:
        daemon_thread(api, config_file)

    if followers is not None:
        followers_processor(api, screen_name=followers)

    return None

if __name__ == '__main__':
    main(docopt(__doc__, version=VERSION))
