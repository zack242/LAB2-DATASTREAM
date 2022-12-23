from kafka import KafkaProducer
import tweepy
import json 
import time

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Set the bearer token
bear_token = "AAAAAAAAAAAAAAAAAAAAAIZWkAEAAAAAeJD3pJ2XdPDP%2B%2FFyfd6RP%2BS9v4g%3DI3wV0t5FcqMiPjoSmpVMw9xbIWSUVtdXtVJTwwjnWnnQ787yDW"

# Define a stream client class that inherits from tweepy's StreamingClient
class MyStream(tweepy.StreamingClient):
        
    def on_connect(self):
        print("Connected")

    # This function gets called when a tweet is received
    def on_tweet(self, tweet):
        # Send the tweet to the 'rawTwitter' topic in Kafka in json format
        producer.send('rawTwitter', json.dumps(dict(tweet), default=str).encode('utf-8'))
        # Sleep for 1 second
        time.sleep(1) 
        return

# Create an instance of the stream client
stream = MyStream(bearer_token=bear_token, wait_on_rate_limit=True, daemon=True)

# Set the rules for filtering tweets
rules = ["macron -is:retweet", "trump -is:retweet"]

# Add the rules to the stream
for rule in rules:
    stream.add_rules(tweepy.StreamRule(value=rule))

# Start streaming tweets that match the rules
stream.filter(expansions=["author_id"], tweet_fields=["created_at","lang"])
