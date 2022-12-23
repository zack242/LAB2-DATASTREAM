import json 
from kafka import KafkaProducer
from kafka import KafkaConsumer

# Initialize Kafka producer and consumer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
consumer = KafkaConsumer('rawTwitter')

# Iterate over the messages in the 'rawTwitter' topic
for msg in consumer:
    # Load the message value as JSON
    res = json.loads(msg.value.decode('utf-8'))
    # Check the language of the tweet
    if res['lang'] == "en":
        # Send the tweet to the 'enTweets' topic if it's in English
        producer.send('enTweets', json.dumps(res, default=str).encode('utf-8'))
    elif res['lang'] == "fr":
        # Send the tweet to the 'frTweets' topic if it's in French
        producer.send('frTweets', json.dumps(res, default=str).encode('utf-8'))

