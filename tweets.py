from twikit import Client , TooManyRequests
import time
from configparser import ConfigParser
from datetime import datetime , timedelta
from random import randint
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import asyncio

#configuring logging
logging.basicConfig(filename='tweets.log', level=logging.INFO)

#configuring the parser
config = ConfigParser()
config.read('config.ini')
username = config['X']['username']
print(username)
password = config['X']['password']
print(password)
email = config['X']['email']
print(email)



# connecting and configuring mongoDB
try:
    mongo_uri = 'mongodb://localhost:27017/tweetsdata'
    client = MongoClient(mongo_uri)
    db = client['twitter_db']
    collection = db['twitter_sentiment_data']
    logging.info("MongoDB connection established")
except KeyError:
    logging.error("MongoDB configuration not found in config.ini")
    print("MongoDB configuration not found in config.ini")
    exit(1)


#count and query for the tweets
today = datetime.utcnow().date()
four_months_ago = today - timedelta(days=120)
count = 10000
QUERY =f"(IPL -filter:retweets lang:en since:2024-12-01"


#creating a twikit client
client = Client(language='en-US')

async def get_tweets(tweets):
    if tweets is None:
        #* get tweets
        print(f'{datetime.now()} - Getting tweets...')
        tweets = await client.search_tweet(QUERY, product='Latest')
    else:
        wait_time = randint(5, 10)
        print(f'{datetime.now()} - Getting next tweets after {wait_time} seconds ...')
        time.sleep(wait_time)
        tweets = await tweets.next()
    
    return tweets


#authenticating the client to twitter and saving cookies.
async def main():

#  await client.login(auth_info_1='Vishnu59241802' , auth_info_2='vishnutwitter2001@gmail.com' , password='Vishnu@twitter123')
#  client.save_cookies('cookies.json')
  client.load_cookies('cookies.json') 

  tweet_count = 0
  tweets = None

  while tweet_count < count:
    try:
        tweets = await get_tweets(tweets)
    except TooManyRequests as e:
        rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
        print(f'{datetime.now()} - Rate limit reached. Waiting until {rate_limit_reset}')
        wait_time = rate_limit_reset - datetime.now()
        time.sleep(wait_time.total_seconds())
        continue

    if not tweets:
        print(f'{datetime.now()} - No more tweets found')
        break

    for tweet in tweets:
        tweet_count += 1
        tweet_data = {
            'id': str(tweet.id),
            'text': tweet.text,
            'created_at': tweet.created_at,
            'author_name': tweet.user.name,
            'retweet_count': tweet.retweet_count,
            'reply_count': tweet.reply_count,
            'view_count': tweet.view_count,
            'retweet_count': tweet.retweet_count,
            'hashtags': tweet.hashtags,
            'retweeted_tweet': tweet.retweeted_tweet,
            'urls': tweet.urls,
            'place': tweet.place.name if tweet.place else None,
            'reply_to': tweet.reply_to
        }
        
        collection.insert_one(tweet_data)
        
        logging.info(f"Inserted {tweet_count} new tweets into MongoDB")
        print(f'{datetime.now()} - Inserted {tweet_count} new tweets into MongoDB')


  


asyncio.run(main())



