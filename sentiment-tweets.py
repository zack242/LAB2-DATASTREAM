import tweepy
import json 
import requests
from kafka import KafkaProducer
from kafka import KafkaConsumer

'''

To perform sentiment analysis on tweets, we are using an external model provided by Hugging Face via an API. 
The model is called "cardiffnlp/twitter-roberta-base-sentiment-latest". We are sending a request to the API 
with the tweet text as input and the API returns the predicted sentiment of the tweet as a response. 
We are then using this predicted sentiment to classify the tweet as positive, negative, or neutral, and sending it to the appropriate Kafka topic.

'''

# Set the model name and Hugging Face token
model = "cardiffnlp/twitter-roberta-base-sentiment-latest"
hf_token = "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" 

# Set the API URL and headers for the request
API_URL = "https://api-inference.huggingface.co/models/" + model
headers = {"Authorization": "Bearer %s" % (hf_token)}

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Define a function for analyzing the sentiment of a tweet
def analysis(data):
    #Send the request to the API
    payload = dict(inputs=data, options=dict(wait_for_model=True))
    response = requests.post(API_URL, headers=headers, json=payload)
    return response.json()

# Initialize the Kafka consumer and subscribe to the 'frTweets' and 'enTweets' topics
consumer = KafkaConsumer()
consumer.subscribe(['frTweets','enTweets'])

for msg in consumer:

    res = json.loads(msg.value.decode('utf-8')) 
    # Get the sentiment with the highest score
    sentiments = max(analysis(res['text'])[0], key=lambda x: x['score'])
    # Convert the sentiment to a dictionary
    top_sentiment = dict(sentiments)

    # Check the label of the sentiment
    if top_sentiment['label'] == 'positive':
        # Send the tweet to the 'positive-tweets' topic if the sentiment is positive
        producer.send('positive-tweets', json.dumps(res, default=str).encode('utf-8'))
    if top_sentiment['label'] == 'negative':
        # Send the tweet to the 'negative-tweets' topic if the sentiment is negative
        producer.send('negative-tweets', json.dumps(res, default=str).encode('utf-8'))
