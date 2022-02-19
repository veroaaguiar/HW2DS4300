# -*- coding: utf-8 -*-

##START OF API##

#READ BEFORE:
#POST TWEET = DONE
#INSERT TWEETS VIA CSV = DONE
#GET TWEETS =
#GET TIMELINE
#GET RANDOM TIMELINES
#GET FOLLOWERS
#GET FOLLOWEES




##importing the appropriate libraries
import pandas as pd
import time
import random as rdm
import redis
from datetime import datetime

# setting host, database, and port for simplicity
# DO NOT HARDCODE!!!!!!!!!
r = redis.Redis(
    host='127.0.0.1', port=6379)


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
        for i in range(num):
            key = f'timeline_of_@{i}'
    # organize by timestamp and limit to 10
            value = r.smembers(key)
            print(key)
            print(value)


    def trial_method(var):
        for i in range(10):
            tweet_text = "hello hello hello"
            #tweet_id = str(i)
            #time_stamp = str(2/14/2022)
            #value = tweet_id + "" + tweet_text + "" + time_stamp
            r.zadd("tweet_18", {tweet_text: str(i)})
        tl = r.zrange("tweet_18", 0, 10, desc=True, withscores=True)
        print(tl)



# CALLING FUNCTIONS
# Tweets.insert_tweets(pd.read_csv('/Users/Vero/Downloads/hw1_data/tweet.csv'))
#Tweets.post_tweet("2", "6897",'I really really hope this works!')
#insert_follows_data()
#Tweets.insert_tweets("/Users/Vero/Downloads/hw1_data/tweet.csv")
#Timelines.get_timeline(6630)
#Timelines.get_random_timeline(rdm.randrange(1,1000,2))
Timelines.trial_method(23)