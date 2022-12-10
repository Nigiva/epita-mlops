from dotenv import load_dotenv
from loguru import logger
import os
import discord
import time
from logger import intercept_logging
from kafka import KafkaConsumer
import json
import asyncio

logger.info("Starting Prediction Consumer")

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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "discordmessage")
logger.info(f"Kafka topic: {KAFKA_TOPIC}")

# Set up logging
logger.add(LOG_PATH, rotation="1 day", retention="1 month", level="DEBUG")
intercept_logging("discord", logger)
intercept_logging("kafka", logger)

# Set up Discord intents
intents = discord.Intents.default()
intents.message_content = True
client = discord.AutoShardedClient(
    intents=intents, 
    shard_count=2,
    max_ratelimit_timeout=30,
)

# Set up Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKER,
    group_id="prediction-consumer",
)
consumer.subscribe(topics=[KAFKA_TOPIC])

async def process_message(channel_id, message_id):
    logger.debug(f"Process message with discord {message_id=} {channel_id=}")
    discord_channel = client.get_channel(channel_id)
    if discord_channel is None:
        logger.error(f"Discord channel {channel_id} not found")
        return
    try:
        discord_message = await discord_channel.fetch_message(message_id)
        await discord_message.add_reaction("\N{WHITE HEAVY CHECK MARK}")
        # await discord_message.add_reaction("\N{NO ENTRY}")
    except discord.errors.NotFound:
        logger.error(f"Discord message {message_id} not found")

@client.event
async def on_ready():
    for message in consumer:
        decoded_message = message.value.decode("utf-8")
        message_obj = json.loads(decoded_message)
        
        channel_id = message_obj["channel_id"]
        message_id = message_obj["message_id"]
        message_content = message_obj["message_content"]
        
        logger.debug(f"Received message {message_id=}")
        await process_message(channel_id, message_id)

client.run(DISCORD_TOKEN)
