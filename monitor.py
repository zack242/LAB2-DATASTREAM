# Import the KafkaConsumer and time modules
from kafka import KafkaConsumer
import time

# Initialize the consumer and subscribe to the 'rawTwitter', 'frTweets', 'enTweets', 'positive-tweets', and 'negative-tweets' topics
consumer = KafkaConsumer()
consumer.subscribe(['rawTwitter', 'frTweets', 'enTweets', 'positive-tweets', 'negative-tweets'])

# Iterate over the messages in the subscribed topics
for message in consumer:
    try:
        # Print the topic, partition, offset, and timestamp of the message
        print("from topic: {}, partition: {}, offset: {}, timestamp: {}".format(
            message.topic, message.partition, message.offset, message.timestamp))
    except Exception as e:
        print("Error: {}".format(e))
    # Sleep for 1 second
    time.sleep(3)
