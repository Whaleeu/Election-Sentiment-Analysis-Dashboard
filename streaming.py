#from config import BEARER_TOKEN
import os 
from dotenv import load_dotenv
from utility import MyStream, create_rules, delete_rules
load_dotenv()
BEARER_TOKEN = os.environ.get("BEARER_TOKEN")


stream = MyStream(BEARER_TOKEN)
delete_rules(stream)
create_rules(stream)

stream.start_stream()