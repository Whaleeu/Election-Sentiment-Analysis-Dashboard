import config
from utility import MyStream, create_rules, delete_rules

stream = MyStream(config.BEARER_TOKEN)
delete_rules(stream)
create_rules(stream)

stream.start_stream()