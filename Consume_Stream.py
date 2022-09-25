from kafka import KafkaProducer, KafkaConsumer
import pandas as pd 
import json
from IPython.display import display
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import time
from datetime import datetime 
from authenticate import BOOTSTRAP_SERVER, SERVER_API_KEY, SERVER_API_SECRET,TOPIC
from dotenv import load_dotenv
load_dotenv()
BEARER_TOKEN = os.environ.get("BEARER_TOKEN")
TABLE_ID = os.environ.get("TABLE_ID")
PROJECT_ID = os.environ.get("PROJECT_ID")




def bin(x):
    if x == 0:
        return 0
    elif x > 0 and x <= 1:
        return  1
    elif x < 0 and x >= -1:
        return -1
    else:
        return None



consume =  KafkaConsumer(TOPIC,bootstrap_servers=BOOTSTRAP_SERVER,value_deserializer=lambda k : k.decode("utf-8"),sasl_plain_username=SERVER_API_KEY,sasl_plain_password=SERVER_API_SECRET,security_protocol="SASL_SSL",sasl_mechanism = "PLAIN")
def Consumer():
    for message in consume:
        output = json.loads(message.value)
        vader = SentimentIntensityAnalyzer()
        predict = vader.polarity_scores(output["tweet"])
        df = pd.DataFrame([{"Candidate": output["tag"],"Tweets":output["tweet"],"Sentiment": predict["compound"],"Date":datetime.now()}])
        df["Map"] = df["Sentiment"].map(bin)
        return df




while True:
    output = Consumer()
    display(output)
    pd.DataFrame.to_gbq(output,TABLE_ID, project_id=PROJECT_ID, if_exists="replace")
    time.sleep(5)


