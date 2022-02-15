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
import random
import redis
from datetime import datetime

# setting host, database, and port for simplicity
# DO NOT HARDCODE
r = redis.Redis(
    host='127.0.0.1', port=6379)


#Additional method for inserting follows data
def insert_follows_data():
    follows = pd.read_csv('/Users/Vero/Downloads/hw1_data/follows.csv')
    print(len(follows))
    for i in range(len(follows)):
        hashName = f'follows_key_{i}'
        user_id = str(follows.loc[i][0])
        follows_id = str(follows.loc[i][1])
        r.hset(hashName, "user_id", user_id)
        r.hset(hashName, "follows_id", follows_id)
        print(hashName)
        print(r.hgetall(hashName))


class Tweets:
    # Insert a single tweet into Tweet table
    def post_tweet(user, text):
        hashName = 'tweet_sample_insert'
        r.hset(hashName, "user", user)
        r.hset(hashName, "tweet_text", text)
        #Time conversion into timestamp
        ts = int(r.time()[0])
        timestamp = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        r.hset(hashName, "time_stamp", str(timestamp))
        print(hashName)
        print(r.hget(hashName, "tweet_text"))
        print(r.hget(hashName, "time_stamp"))

    # INSERT STATEMENT INTO TWEET TABLE
    def insert_tweets(csv):
        tweets = pd.read_csv(csv)
        for i in range(len(tweets)):
            hashName = f'tweet_id_{i}'
            user_id = str(tweets.loc[i][0])
            r.hset(hashName, "user_id", user_id)
            r.hset(hashName, "tweet_text", tweets.loc[i][1])
            # Time conversion into timestamp
            ts = int(r.time()[0])
            timestamp = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            r.hset(hashName, "time_stamp", timestamp)
            print(r.hgetall(hashName))

   #still need to do
    # Retrieve tweets from person with given user_id
    def get_tweets(user_id):
        query = "SELECT * FROM TWEET WHERE TWEET.user_id = (%s) ORDER BY tweet_ts DESC"
        value = [user_id, ]
        cursor.execute(query, value[0])
        cnx.commit()
        records = cursor.fetchmany(10)
        for row in records:
            print("User: ", row[1])
            print("Tweet:", row[2])
            print("Time Posted: ", row[3])
            print("\n")
        cursor.close()


class Timelines:
    #Still need to do
    # Get a single timeline
    def get_timeline(user_id):
        cursor = cnx.cursor(buffered=True)
        query = "SELECT TWEET.tweet_text, TWEET.tweet_ts, TWEET.user_id from TWEET JOIN FOLLOWS ON FOLLOWS.follows_id = TWEET.user_id WHERE FOLLOWS.user_id = (%s) ORDER BY tweet_ts DESC"
        value = [user_id, ]
        cursor.execute(query, value[0])
        cnx.commit()
        records = cursor.fetchmany(10)
        print("Your Timeline: \n")
        for row in records:
            print("User: ", row[2])
            print("Tweet:", row[0])
            print("Time Posted: ", row[1])
            print("\n")
        cursor.close()

# Get various random timelines
    def get_random_timeline(self):
        cursor = cnx.cursor(buffered=True)
        for i in range(1000):
            random_id = random.randrange(1,100,2)
            query = "SELECT TWEET.tweet_text, TWEET.tweet_ts, TWEET.user_id from TWEET JOIN FOLLOWS ON FOLLOWS.follows_id = TWEET.user_id WHERE FOLLOWS.user_id = (%s) ORDER BY tweet_ts DESC"
            value = [(random_id)]
            cursor.execute(query, (value[0],))
            cnx.commit()
            print("User's Timeline: " + str(random_id))
            records = cursor.fetchmany(10)
            print("Your Timeline:")
            for row in records:
                print("User: ", row[2])
                print("Tweet:", row[0])
                print("Time Posted: ", row[1])
                print("\n")
        cursor.close()


class Followers:
# WHO IS FOLLOWING USER_ID?
   def get_followers(user_id):
    query = "SELECT FOLLOWS.user_id FROM FOLLOWS WHERE FOLLOWS.follows_id = (%s)"
    value = [user_id,]
    cursor.execute(query, (value[0],))
    cnx.commit()
    records = cursor.fetchmany(100)
    for row in records:
        print("Follower: ", row[0])

    cursor.close()

# WHO IS USER_ID following?
    def get_followees(user_id):
        query = "SELECT FOLLOWS.follows_id FROM FOLLOWS WHERE FOLLOWS.user_id = (%s)"
        value = [user_id,]
        cursor.execute(query, (value[0],))
        cnx.commit()
        records = cursor.fetchmany(100)
        for row in records:
            print("Following: ", row[0])

        cursor.close()


# CALLING FUNCTIONS
# Tweets.insert_tweets(pd.read_csv('/Users/Vero/Downloads/hw1_data/tweet.csv'))
Tweets.post_tweet("Vero", "never rains in California")
#insert_follows_data()