from kafka import KafkaProducer
import tweepy
import ast
import time 
import json

from config import KAFKA_SERVER, TOPIC_NAME

bat_rule_1 = "(Tinubu OR Asiwaju) lang:en"
bat_rule_2 = "(#BAT2023 OR #ABAT2023) lang:en"

obi_query_1 = "(#Obidients OR #Obidatti2023) lang:en"
obi_query_2 = "(PeterObi OR ((Obidient OR Obidients) (LP OR Labour Party))) lang:en"

atk_query_1 = "(#AtikuOkowa2023 OR #Atiku) lang:en"
atk_query_2 = "Atiku lang:en"


producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)


def send_message(msg, topic=TOPIC_NAME):
    send_message.called = True
    producer.send(topic, msg)
    producer.flush()


def package_message(raw):
    tweet_data = ast.literal_eval(raw.decode('UTF-8'))

    tweet_dict = dict(tweet=tweet_data['data']['text'], tag=tweet_data['matching_rules'][0]['tag'])

    return json.dumps(tweet_dict, indent=2).encode('utf-8')



import time
import time

class MyStream(tweepy.StreamingClient):
    def on_connect(self):
        print("Connected to Stream!!!\n ===================")
        

    def on_data(self, raw_data):

        message = package_message(raw_data)

        send_message(message)
        print(message)

        time.sleep(1)


def delete_rules(stream):
    rules = stream.get_rules().data
    rules_ids =[]
    for rule in rules:
        rules_ids.append(rule.id)

    stream.delete_rules(rules_ids)

def create_rules(stream):
    stream.add_rules(tweepy.StreamRule(bat_rule_1, tag="BAT", ))
    stream.add_rules(tweepy.StreamRule(obi_query_1, tag="OBI"))
    stream.add_rules(tweepy.StreamRule(atk_query_1, tag="ATK"))
    stream.add_rules(tweepy.StreamRule(bat_rule_2, tag="BAT"))
    stream.add_rules(tweepy.StreamRule(obi_query_2, tag="OBI"))
    stream.add_rules(tweepy.StreamRule(atk_query_2, tag="ATK"))

