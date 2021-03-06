# -*- coding: utf-8 -*-

##START OF API##





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
        r.lpush(key, value)
        print(key + str(r.lrange(key, 0, 200)))
    finish = time.time()
    diff = finish - start
    rate = len(follows) / diff
    print(f'Stored {len(follows)} keys in {diff} seconds (Rate={rate}/sec')


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
        r.lpush(key, value)

        #Insert into timeline automatically
        followers = f'user_{user_id}_is_followed_by'
        followers_list = r.lrange(followers, 0, 200 )
        for i in followers_list:
            member = i
            key = f'timeline_of_@{member}'
            tl_value = "tweet: " + value
            r.lpush(key, tl_value)
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

            #Insert into timelines
            followers = f'user_{user_id}_is_followed_by'
            followers_list = r.lrange(followers, 0, 3000)
            print(followers_list)
            for i in followers_list:
                member = str(i)
                member_cleaned_pre = member.replace("b'", "")
                member_cleaned = member_cleaned_pre.replace("'", "")
                key = f'timeline_of_@{member_cleaned}'
                tl_value = "tweet: " + value
                r.lpush(key, tl_value)
                print(key + " : " + value)


        finish = time.time()
        diff = finish - start
        rate = len(tweets) / diff
        print(f'Stored {len(tweets)} keys in {diff} seconds (Rate={rate}/sec)')




class Timelines:
    # Get a single timeline
    def get_timeline(user_id):
        key = f'timeline_of_@{user_id}'
        value = r.lrange(key, 0, 10,)
        print(key + ": \n" + str(value))



# Get various random timelines
    def get_random_timeline(num):
        start = time.time()
        for i in range(num):
            random_tl = random.randint(1, 10000)
            key = f'timeline_of_@{random_tl}'
            T10_timeline =r.lrange(key, 0, 9)
            print(key + ": \n" + str(T10_timeline))
        finish = time.time()
        diff = finish - start
        rate = num / diff
        print(f'Retrieved {num} timelines in {diff} seconds (Rate={rate}/sec)')





# CALLING FUNCTIONS
# Tweets.insert_tweets(pd.read_csv('/Users/Vero/Downloads/hw1_data/tweet.csv'))
#Tweets.post_tweet("2", "6897",'I really really hope this works!')
#insert_follows_data()
#Tweets.post_tweet("3", "2", "hello im trying this out")
#Tweets.insert_tweets("/Users/Vero/Downloads/hw1_data/tweet.csv")
#Timelines.get_timeline(6630)
#Timelines.get_random_timeline(200000)
#Timelines.trial_method(23)