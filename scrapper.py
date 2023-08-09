
from tweepy import StreamRule
from tweepy import StreamingClient
from kafka import KafkaProducer
from config import *
import json

class KafkaProducerListener(StreamingClient):
    def __init__(self, bearer_token):
        StreamingClient.__init__(self, bearer_token)
        self.producer = KafkaProducer(bootstrap_servers=[kafka_url])

    def on_data(self, data):
        data = json.loads(data.decode()) 
        data = data["data"]["text"]
        
        print(data)
        self.producer.send(topic_name, data.encode('utf-8'))

        return True

    def on_error(self, status):
        print(status)
        return True

streaming_client = KafkaProducerListener(bearer_token)

hastag = "#ipl2022"

streaming_client.add_rules(StreamRule(value=hastag)) 

streaming_client.filter( tweet_fields=["text","created_at","entities"])