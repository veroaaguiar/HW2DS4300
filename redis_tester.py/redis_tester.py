"""
Created on Thu Feb  4 21:56:24 2021

@author: rachlin
@file  : redis_test.py
"""
import redis
import pandas as pd


tweets = pd.read_csv("/Users/Vero/Downloads/tweets_sample.csv")
print(tweets)
print(tweets.loc[2][1])


#%% Create a connection and clear the database
r = redis.Redis(host='localhost', port=6379, db = 0, decode_responses=True)
r.flushall()

# #%% Some random tests
# r.set('foo', 100)
# x = r.get('foo')
# print('foo', x)
#
# r.lpush('friends', 'joe')
# r.lpush('friends', 'bob')
# r.lpush('friends', 'cal')
#
# friends = r.lrange('friends', 0, -1)
#
# print(friends)

redis.Redis()


# #%% Store 1 million keys

import time

r.flushall()
N = 18

def test_run():
 start = time.time()
 for i in range(N):
     r.set('key:' + str(i), 'This is tweet ' + str(i))
     r.set(f'key:{i}',i)
     print('key:'+str(i))
 finish = time.time()
 diff = finish - start
 rate = N / diff
 print(f'Stored {N} keys in {diff} seconds (Rate={rate}/sec')



import datetime
today = datetime.date.today()
stoday = today.isoformat()
start = time.time()

# ##Post Tweet
# hashName = 'tweet_1'
# r.hset(hashName, "user", "Vero")
# r.hset(hashName, "tweet_text", "Helloooo")
# r.hset(hashName, "time_stamp", stoday)
# print(hashName)
# print(r.hget(hashName, "tweet_text"))
# print(r.hget(hashName, "time_stamp"))
# finish = time.time()
# diff = finish - start


def insert_tweets(csv):
    tweets = pd.read_csv(csv)
    for i in range(len(tweets)):
     hashName = f'tweet_id_{i}'
     user_id = str(tweets.loc[i][0])
     r.hset(hashName, "user_id", user_id)
     r.hset(hashName, "tweet_text", tweets.loc[i][1])
     r.hset(hashName, "time_stamp", stoday)
     print(r.hgetall(hashName))


# v = 10;
# for i in range(v):
#     hashName = f'tweet_id_{i}'
#     r.hset(hashName, "user", "Vero")
#     r.hset(hashName, "tweet_text", "Helloooo")
#     r.hset(hashName, "time_stamp", stoday)
#     print(hashName)
#     print(r.hget(hashName, "tweet_text"))
#     print(r.hget(hashName, "time_stamp"))
# finish = time.time()
# diff = finish - start
# rate = N / diff


#insert_tweets("/Users/Vero/Downloads/tweets_sample.csv")


#Inserting follows data
def insert_follows_data():
    follows = pd.read_csv('/Users/Vero/Downloads/follows_sample.csv')
    print(len(follows))
    for i in range(len(follows)):
        hashName = f'follows_key_{i}'
        user_id = str(follows.loc[i][0])
        follows_id = str(follows.loc[i][1])
        r.hset(hashName, "user_id", user_id)
        r.hset(hashName, "follows_id", follows_id)
        print(r.hgetall(hashName))




