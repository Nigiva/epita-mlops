from dotenv import load_dotenv
from loguru import logger
import os

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

