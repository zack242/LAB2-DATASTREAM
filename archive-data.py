from kafka import KafkaConsumer

consumer = KafkaConsumer()
consumer.subscribe(['rawTwitter','frTweets','enTweets','positive-tweets','negative-tweets'])

for msg in consumer : 
    f = open("archive.txt", "a")
    f.write(str(msg))
    f.write("\n")
    f.close()