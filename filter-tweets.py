import json 
from kafka import KafkaProducer
from kafka import KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
consumer = KafkaConsumer('rawTwitter')

for msg in consumer:
    res = json.loads(msg.value.decode('utf-8'))
    if res['lang'] == "en" : 
        producer.send('enTweets',json.dumps(res, default=str).encode('utf-8'))
    elif res['lang'] == "fr" : 
        producer.send('frTweets',json.dumps(res, default=str).encode('utf-8'))
