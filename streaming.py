import config
from utility import MyStream, create_rules, delete_rules

stream = MyStream(config.BEARER_TOKEN)
delete_rules(stream)
create_rules(stream)

stream.filter(tweet_fields=['referenced_tweets'])
