##START OF API##

#################################

# This is NOT our final code. However, we tried implementing the database in two ways: Sets, and Lists. Below is the
# code for lists, that returns ALL members of all timelines. Performance below:
# INSERT FOLLOWS: Stored 30032 keys in 10.889110803604126 seconds (Rate=2757.9846088130425/sec)
# INSERT TWEETS: Stored 1000000 keys in 693.7739980220795 seconds (Rate=1441.391581193527/sec)
# Couldn't time get timelines because I had already switched method of insertion to lists before being able to call this
# method :)

#################################




##importing the appropriate libraries
import random
import pandas as pd
import time
import redis
from datetime import datetime


# setting host, database, and port for simplicity
# DO NOT HARDCODE!!!!!!!!!
host_var = '127.0.0.1'
port_var = 6379
r = redis.Redis(
    host=host_var, port=port_var)


#Additional method for inserting follows data
def insert_follows_data():
    start = time.time()
    follows = pd.read_csv('/Users/Vero/Downloads/hw1_data/follows.csv')
    print(len(follows))
    for i in range(len(follows)):
        user_id = str(follows.loc[i][0])
        follows_id = str(follows.loc[i][1])
        key = f'user_{follows_id}_is_followed_by'
        value = user_id
        r.sadd(key, value)
        print(key + str(r.smembers(key)))
    finish = time.time()
    diff = finish - start
    rate = len(follows) / diff
    print(f'Stored {len(follows)} keys in {diff} seconds (Rate={rate}/sec)')


class Tweets:
    # Insert a single tweet into Tweet table
    def post_tweet(tweet, user, text):
        start = time.time()
        tweet_id = tweet
        user_id = user
        tweet_text = text
        # Time conversion into timestamp
        ts = int(r.time()[0])
        timestamp = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        #Insertion into redis
        key = f'tweet_{tweet_id}_by_user_{user_id}'
        value = tweet_id + " " + user_id + " " + tweet_text + " " + timestamp
        r.sadd(key, value)

        #Insert into timeline
        followers = f'user_{user_id}_is_followed_by'
        followers_list = list(r.smembers(followers))
        for i in followers_list:
            member = i
            key = f'timeline_of_@{member}'
            tl_value = "tweet: " + value
            r.sadd(key, tl_value)
            print(member)
            print(key + " : " + value)

        print(followers_list)





    # INSERT STATEMENT INTO TWEET TABLE
    def insert_tweets(csv):
        start = time.time()
        tweets = pd.read_csv(csv)
        for i in range(len(tweets)):
            tweet_id = str(i)
            user_id = str(tweets.loc[i][0])
            tweet_text = str(tweets.loc[i][1])
            # Time conversion into timestamp
            ts = int(r.time()[0])
            timestamp = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

            #Insertion into redis
            key = f'tweet_{i}_by_@{user_id}'
            value = tweet_id + " " + user_id + " " + tweet_text + " " + timestamp
            print(key + " : " + value)

            #Insert into timelines!
            followers = f'user_{user_id}_is_followed_by'
            followers_list = list(r.smembers(followers))
            for i in followers_list:
                member = str(i)
                member_cleaned_pre = member.replace("b'", "")
                member_cleaned = member_cleaned_pre.replace("'", "")
                print(member_cleaned)
                key = f'timeline_of_@{member_cleaned}'
                tl_value = "tweet: " + value
                r.sadd(key, tl_value)
                print(key + " : " + value)

        finish = time.time()
        diff = finish - start
        rate = len(tweets) / diff
        print(f'Stored {len(tweets)} keys in {diff} seconds (Rate={rate}/sec)')




class Timelines:
    # Get a single timeline
    def get_timeline(user_id):
        #get the list of user_id's followers
        #create new timeline for each of them
        #call get tweets
        key = f'timeline_of_@{user_id}'
        #organize by timestamp and limit to 10
        value = r.smembers(key)
        print(key)
        print(value)



# Get various random timelines
    def get_random_timeline(num):
        start = time.time()
        for i in range(num):
            random_tl = random.randint(1, 10000)
            key = f'timeline_of_@{random_tl}'
            value = r.smembers(key)
            print(key)
            print(value)
        finish = time.time()
        diff = finish - start
        rate = num / diff
        print(f'Retrieved {num} timelines in {diff} seconds (Rate={rate}/sec)')





# CALLING FUNCTIONS
# Tweets.insert_tweets(pd.read_csv('/Users/Vero/Downloads/hw1_data/tweet.csv'))
#Tweets.post_tweet("2", "6897",'I really really hope this works!')
#insert_follows_data()
#Tweets.insert_tweets("/Users/Vero/Downloads/hw1_data/tweet.csv")
Timelines.get_timeline(2)
#Timelines.get_random_timeline(200000)
print(random.randint(1, 10000))