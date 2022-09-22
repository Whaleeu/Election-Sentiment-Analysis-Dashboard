from dotenv import load_dotenv
import os
from Consume_Stream import load_gbq
from Consume_Stream import Consumer
from Consume_Stream import load_gbq





load_dotenv()
BEARER_TOKEN = os.environ.get("BEARER_TOKEN ")
TABLE_ID = os.environ.get("TABLE_ID")
PROJECT_ID = os.environ.get("PROJECT_ID")
KAFKA_SERVER = os.environ.get("KAFKA_SERVER")
TOPIC_NAME =  os.environ.get("TOPIC_NAME")


Data = Consumer(TOPIC_NAME,KAFKA_SERVER)   # Topic_name : TOPIC_NAME , kafka_server : KAFKA_SERVER
load_gbq(Data,TABLE_ID,PROJECT_ID,5)  # dataframe : Data, table_id: TABLE_ID,project_id:PROJECT_ID,sleep: 5sec