from kafka import KafkaConsumer
import json
import logging

logger = logging.getLogger("kafka")
logger.addHandler(logging.StreamHandler())

topic = "discordmessage" # exo1
consumer = KafkaConsumer(topic, group_id="discordmessage", bootstrap_servers="127.0.0.1:9092") # 51.38.185.58
for msg in consumer:
    if msg is not None:
        message = msg.value.decode("utf-8")
        print("\n\n=====================================")
        print(message)
        try:
            print(">>> Json format :", json.loads(message))
        except:
            print("Not a json format")