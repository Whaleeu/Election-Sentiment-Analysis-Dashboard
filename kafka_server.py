
from kafka import KafkaProducer
from authenticate import BOOTSTRAP_SERVER, SERVER_API_KEY, SERVER_API_SECRET

producer = KafkaProducer(bootstrap_servers = BOOTSTRAP_SERVER,
                                security_protocol= "SASL_SSL",
                                sasl_mechanism = "PLAIN",
                                sasl_plain_username= SERVER_API_KEY,
                                sasl_plain_password= SERVER_API_SECRET)