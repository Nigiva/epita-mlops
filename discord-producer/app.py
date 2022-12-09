from dotenv import load_dotenv
from loguru import logger
import os
import discord
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
LOG_PATH = os.getenv("LOG_PATH")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "discordmessage")

# Set up logging
logger.add(LOG_PATH, rotation="1 day", retention="1 month", level="DEBUG")
intercept_logging("discord", logger)
intercept_logging("kafka", logger)

# Check the presence of the Discord token
if DISCORD_TOKEN is None or DISCORD_TOKEN == "":
    logger.critical("No Discord token found")
    exit(1)

# Set up Discord intents
intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

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
        "id": message.id,
        "content": message.content,
    }
    message_json = json.dumps(message_object)
    producer.send(KAFKA_TOPIC, message_json.encode("utf-8"))
    
    await message.add_reaction("\N{WHITE HEAVY CHECK MARK}")
    # await message.add_reaction("\N{NO ENTRY}")
    
client.run(DISCORD_TOKEN)