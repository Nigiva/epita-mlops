from dotenv import load_dotenv
from loguru import logger
import os
import sys
import discord
import logging

# Handler for logging to Loguru
class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level if it exists.
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

# Intercept Discord logging
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.DEBUG)
discord_logger.addHandler(InterceptHandler())

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

# Set up logging
logger.add(LOG_PATH, rotation="1 day", retention="1 month", level="DEBUG")

# Check the presence of the Discord token
if DISCORD_TOKEN is None or DISCORD_TOKEN == "":
    logger.critical("No Discord token found")
    exit(1)

# Set up Discord intents
intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

# Event handler for when the bot is ready
@client.event
async def on_ready():
    logger.success("Discord Producer is ready")

@client.event
async def on_message(message):
    if message.author == client.user:
        return
    
    logger.debug(f"Received message from {message.author}: {message.content}")
    await message.add_reaction("\N{WHITE HEAVY CHECK MARK}")
    await message.add_reaction("\N{NO ENTRY}")
    
client.run(DISCORD_TOKEN)