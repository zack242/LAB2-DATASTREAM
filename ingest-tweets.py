import tweepy
import json 
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

token = "AAAAAAAAAAAAAAAAAAAAAIZWkAEAAAAAeJD3pJ2XdPDP%2B%2FFyfd6RP%2BS9v4g%3DI3wV0t5FcqMiPjoSmpVMw9xbIWSUVtdXtVJTwwjnWnnQ787yDW"
client = tweepy.Client(bearer_token=token)
query = "elonmusk -is:retweet"

while True : 
    for tweet in tweepy.Paginator(client.search_recent_tweets, query=query,tweet_fields=[ 'created_at', 'lang', 'possibly_sensitive'], max_results=100).flatten(limit=10):
            producer.send('rawTwitter',json.dumps(dict(tweet), default=str).encode('utf-8'))
    time.sleep(60) 