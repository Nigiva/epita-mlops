from dotenv import load_dotenv
from loguru import logger
import os
import discord
from discord.ext import tasks
from logger import intercept_logging
from kafka import KafkaConsumer
import time
import json
import logging
import threading
from collections import deque

logger.info("Starting Drift Detector")

# Load .env file
logger.info("Loading .env file")
if load_dotenv():
    logger.success("Loaded .env file")
else:
    logger.warning("Failed to load .env file")

# Get environment variables
logger.info("Loading environment variables")
LOG_PATH = os.getenv("LOG_PATH")
logger.info(f"Log path: {LOG_PATH}")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
if DISCORD_TOKEN is None or DISCORD_TOKEN == "":
    logger.critical("No Discord token found")
    exit(1)
else:
    logger.info(f"Discord token: ***")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
logger.info(f"Kafka broker: {KAFKA_BROKER}")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "modelprediction")
logger.info(f"Kafka topic: {KAFKA_TOPIC}")
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", "1000"))
logger.info(f"Buffer size: {BUFFER_SIZE}")
MINUTES_BETWEEN_ITERATIONS = int(os.getenv("MINUTES_BETWEEN_ITERATIONS", "5"))
logger.info(f"Minutes between iterations : {MINUTES_BETWEEN_ITERATIONS}")
MONITORING_CHANNEL_ID = int(os.getenv("MONITORING_CHANNEL_ID"))
logger.info(f"Channel {MONITORING_CHANNEL_ID} is the monitoring channel")
DEBUG_MODE = os.getenv("DEBUG_MODE", "False") == "True"
logger.info(f"Debug mode: {DEBUG_MODE}")
WAIT_FOR_KAFKA = int(os.getenv("WAIT_FOR_KAFKA", 10))
logger.info(f"Wait for Kafka (secondes): {WAIT_FOR_KAFKA}")

# Set up logging
logger.add(LOG_PATH, rotation="1 day", retention="1 month", level="DEBUG")
intercept_logging("discord", logger, level=logging.ERROR)
intercept_logging("kafka", logger)

# Set up buffer
buffer = deque(maxlen=BUFFER_SIZE)

# Set up Discord intents
intents = discord.Intents.default()
intents.message_content = True
client = discord.AutoShardedClient(
    intents=intents, 
    shard_count=3,
)

# Set up Kafka consumer
logger.info("Waiting for Kafka broker to be ready")
time.sleep(WAIT_FOR_KAFKA)

consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKER,
    group_id="drift-detector",
    auto_offset_reset="earliest",
)
consumer.subscribe(topics=[KAFKA_TOPIC])

# Discord client events
@tasks.loop(minutes=MINUTES_BETWEEN_ITERATIONS, reconnect=True)
async def check_for_drift(channel):
    logger.info("Checking for drift")
    if len(buffer) < BUFFER_SIZE:
        logger.warning("Buffer not full, ignoring check")
        return
    
    current_buffer = list(buffer.copy())
    await channel.send("Test")
    logger.success("Drift check is finished")

@client.event
async def on_ready():
    logger.debug(f"Getting Discord Channel {MONITORING_CHANNEL_ID}")
    channel = client.get_channel(MONITORING_CHANNEL_ID)
    if channel is None:
        logger.critical(f"Discord channel {MONITORING_CHANNEL_ID} not found")
        return
    
    check_for_drift.start(channel)

# Add predictions to buffer
def stream_to_buffer():
    logger.info("Streaming predictions to buffer")
    for prediction in consumer:
        prediction_str = prediction.value.decode("utf-8")
        prediction_obj = json.loads(prediction_str)
        message_id = prediction_obj["message_id"]
        logger.debug(f"Received message {message_id}")
        
        buffer.append(prediction_obj)
        consumer.commit()
threading.Thread(target=stream_to_buffer).start()

client.run(DISCORD_TOKEN)