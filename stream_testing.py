import tweepy
from kafka import KafkaProducer, KafkaConsumer
import time
import config

bat_query = "#BAT2023 OR #ABAT2023 OR Tinubu OR Asiwaju lang:en"

obi_query = "#Obidients OR #Obidatti2023 OR PeterObi OR ((Obidient OR Obidients) (LP OR Labour Party)) lang:en"

atk_query = "#AtikuOkowa2023 OR #Atiku OR Atiku lang:en"


TOPIC_NAME = "twitter-bat"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

def send_message(msg):
    send_message.called = True
    producer.send(TOPIC_NAME, msg)
    producer.flush()




class MyStream(tweepy.StreamingClient):
    def on_connect(self):
        print("Connected to Stream!!!\n ===== ===== ===== =====")
        return super().on_connect()

    def on_tweet(self, tweet):
        send_message.called = False
        if tweet.referenced_tweets ==None:
            msg = tweet.text
            msg_b = bytes(msg, "utf-8")
            #print(tweet.text)
            send_message(msg_b)
            if send_message.called == True:
                print("Message sent!")

        time.sleep(1)

mystream = MyStream(config.BEARER_TOKEN)

mystream.add_rules(tweepy.StreamRule(bat_query))

mystream.filter(tweet_fields=['referenced_tweets'])