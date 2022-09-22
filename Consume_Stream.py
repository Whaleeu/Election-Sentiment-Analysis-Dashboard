from kafka import KafkaProducer, KafkaConsumer
import pandas as pd 
import json
from IPython.display import display
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import time
from datetime import datetime





def bin(x):
    if x == 0:
        return 0
    elif x > 0 and x <= 1:
        return  1
    elif x < 0 and x >= -1:
        return -1
    else:
        return None




def Consumer(Topic_name,kafka_server):
    consume =  KafkaConsumer(Topic_name,bootstrap_servers=kafka_server,value_deserializer=lambda k : k.decode("utf-8") )
    for message in consume:
        output = json.loads(message.value)
        vader = SentimentIntensityAnalyzer()
        predict = vader.polarity_scores(output["tweet"])
        df = pd.DataFrame([{"Candidate": output["tag"],"Tweets":output["tweet"],"Sentiment": predict["compound"],"Date":datetime.now()}])
        df["Map"] = df["Sentiment"].map(bin)
        return df




def load_gbq(dataframe,table_id ,project_id,sleep):
    while True:
        pd.DataFrame.to_gbq(dataframe,table_id=table_id, project_id=project_id, if_exists="replace")
        time.sleep(sleep)

