from kafka import KafkaConsumer

# Initialize the consumer and subscribe to the 'rawTwitter', 'frTweets', 'enTweets', 'positive-tweets', and 'negative-tweets' topics
consumer = KafkaConsumer()
consumer.subscribe(['rawTwitter', 'frTweets', 'enTweets', 'positive-tweets', 'negative-tweets'])

# Iterate over the messages in the subscribed topics
for msg in consumer:
    # Open the 'archive.txt' file in append mode
    with open("archive.txt", "a") as f:
        # Write the message to the file
        f.write(str(msg))
        # Add a newline character
        f.write("\n")
    # Close the file after writing to it
    f.close()
