from dotenv import load_dotenv
from loguru import logger
import os
import discord
import time
from logger import intercept_logging
from kafka import KafkaProducer
import json

logger.info("Starting Discord Producer")

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
DEBUG_MODE = os.getenv("DEBUG_MODE", "False") == "True"
logger.info(f"Debug mode: {DEBUG_MODE}")
WAIT_FOR_KAFKA = int(os.getenv("WAIT_FOR_KAFKA", 10))
logger.info(f"Wait for Kafka (secondes): {WAIT_FOR_KAFKA}")

# Set up logging
logger.add(LOG_PATH, rotation="1 day", retention="1 month", level="DEBUG")
intercept_logging("discord", logger)
intercept_logging("kafka", logger)

# Set up Discord intents
intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(
    intents=intents,
    shard_count=3,
)

# Set up Kafka producer
logger.info("Waiting for Kafka broker to be ready")
time.sleep(WAIT_FOR_KAFKA)

logger.info("Setting up Kafka producer")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
)

# Event handler for when the bot is ready
@client.event
async def on_ready():
    logger.success("Discord Producer is ready")

@client.event
async def on_message(message):
    if message.author == client.user:
        return
    
    logger.debug(f"Received message from {message.author}: {message.content}")
    message_object = {
        "channel_id": message.channel.id,
        "message_id": message.id,
        "message_content": message.content,
    }
    message_json = json.dumps(message_object)
    producer.send(KAFKA_TOPIC, message_json.encode("utf-8"))
    logger.info(f"Sent message {message.id} to Kafka")
    
    if DEBUG_MODE:
        await message.add_reaction("\N{EYES}")
    
client.run(DISCORD_TOKEN)