from kafka import KafkaProducer
import time
import logging

logger = logging.getLogger("kafka")
logger.addHandler(logging.StreamHandler())

producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")
topic="discordmessage"

# 5/ Send a message to the topic
producer.send(topic, "coucou Duchene".encode("utf-8"))
producer.flush()