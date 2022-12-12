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
import datetime
from matplotlib import pyplot as plt
import io
import eurybia
import pandas as pd
import functools
import typing
import asyncio

MODERATOR_IMAGE_URL = "https://cdn.discordapp.com/app-icons/1050840682492870686/99b2bdf30af9f3cd2a72d52895178986.png"

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
PREDICTION_BUFFER_SIZE = int(os.getenv("PREDICTION_BUFFER_SIZE", "1000"))
logger.info(f"Buffer size: {PREDICTION_BUFFER_SIZE}")
AUC_BUFFER_SIZE = int(os.getenv("AUC_BUFFER_SIZE", "1000"))
logger.info(f"Buffer size: {AUC_BUFFER_SIZE}")
MINUTES_BETWEEN_ITERATIONS = int(os.getenv("MINUTES_BETWEEN_ITERATIONS", "5"))
logger.info(f"Minutes between iterations : {MINUTES_BETWEEN_ITERATIONS}")
MONITORING_CHANNEL_ID = int(os.getenv("MONITORING_CHANNEL_ID"))
logger.info(f"Channel {MONITORING_CHANNEL_ID} is the monitoring channel")
WAIT_FOR_KAFKA = int(os.getenv("WAIT_FOR_KAFKA", 10))
logger.info(f"Wait for Kafka (secondes): {WAIT_FOR_KAFKA}")

# Set up logging
logger.add(LOG_PATH, rotation="1 day", retention="1 month", level="DEBUG")
intercept_logging("discord", logger, level=logging.ERROR)
intercept_logging("kafka", logger)

# Set up buffer
prediction_buffer = deque(maxlen=PREDICTION_BUFFER_SIZE)
auc_buffer = deque(maxlen=AUC_BUFFER_SIZE)

# Load subset of embedding extracted from training data
train_embedding_df = pd.read_csv("data/train_embedding.csv", index_col=0)
# Fix column names which are strings by default in reading from CSV
train_embedding_df.columns = train_embedding_df.columns.astype("int64")

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

def to_thread(func: typing.Callable) -> typing.Coroutine:
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        wrapped_func = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, wrapped_func)
    return wrapper


def from_prediction_list_to_dataframe(prediction_list):
    embedding_list = [prediction["sentence_embedding"] for prediction in prediction_list]
    return pd.DataFrame(embedding_list)

@to_thread
def get_data_drift(prediction_list):
    logger.info("Computing data drift")
    
    logger.debug(f"Converting list of prediction objects to dataframe")
    current_embedding_df = from_prediction_list_to_dataframe(prediction_list)
    logger.debug(f"Dataframe shape: {current_embedding_df.shape}")
    logger.debug(f"Instantiating SmartDrift")
    sd = eurybia.SmartDrift(
        df_current=current_embedding_df,
        df_baseline=train_embedding_df,
    )
    
    # Compile the drift detector (1 to 5 minutes)
    logger.info(f"Compiling SmartDrift (1 to 5 minutes)")
    sd.compile()
    
    logger.success(f"SmartDrift has been compiled and the drift score is AUC={sd.auc}")
    return sd.auc

@to_thread
def generate_auc_embed(auc_score, datetime_str):
    # Generate image
    data_stream = io.BytesIO()
    smart_plotter = eurybia.core.smartplotter.SmartPlotter(smartdrift=None)
    plotly_figure = smart_plotter.generate_indicator(auc_score)
    image_byte = plotly_figure.to_image(format="png", width=500, height=300, scale=1)
    data_stream.write(image_byte)
    
    # Generate embed
    data_stream.seek(0)
    chart_file = discord.File(data_stream, filename="auc_chart.png")
    embed=discord.Embed(
        title="Moderation Report",
        color=0xf5c211
    )
    embed.set_image(
        url="attachment://auc_chart.png"
    )
    embed.add_field(name="Data Drift Score", value=str(auc_score), inline=False)
    embed.set_footer(text=datetime_str)
    
    return embed, chart_file

@to_thread
def generate_auc_evolution_embed(datetime_str):
    auc_list = list(auc_buffer.copy())
    data_stream = io.BytesIO()
    
    # Generate chart
    plt.figure(figsize=(8,3))
    plt.plot(auc_list)
    plt.title("AUC Evolution")
    plt.xlabel("Iterations")
    plt.xticks([i + 1 for i in range(len(auc_list))])
    plt.ylabel("AUC Score")
    plt.savefig(data_stream, format="png", bbox_inches="tight", dpi = 80)
    plt.close()
    
    # Generate embed
    data_stream.seek(0)
    chart_file = discord.File(data_stream, filename="auc_evolution_chart.png")
    
    embed=discord.Embed(
        title="Moderation Report", 
        description="Data Drift Score Evolution",
        color=0x1c71d8,
    )
    embed.set_image(
        url="attachment://auc_evolution_chart.png"
    )
    embed.set_footer(text=datetime_str)
    
    return embed, chart_file

# Discord client events
@tasks.loop(minutes=MINUTES_BETWEEN_ITERATIONS, reconnect=True)
async def check_for_drift(channel):
    logger.info("Checking for drift")
    now = datetime.datetime.now()
    datetime_str = now.strftime("%d/%m/%Y - %H:%M:%S")
    current_buffer = list(prediction_buffer.copy())
    
    if len(current_buffer) < PREDICTION_BUFFER_SIZE:
        logger.warning("Buffer not full, ignoring check")
        return
    
    # Remove last element to make sure we don't predict on the same data twice
    # So we force to wait a new example to have the buffer full
    prediction_buffer.pop() 
    
    logger.info("Computing Data Drift")
    auc_score = await get_data_drift(current_buffer)
    auc_buffer.append(auc_score)
    logger.success("Data Drift has been computed")
    auc_score_embed, auc_file = await generate_auc_embed(auc_score, datetime_str)
    logger.success("AUC embed has been generated")
    auc_evolution_embed, auc_evolution_file = await generate_auc_evolution_embed(datetime_str)
    logger.success("AUC evolution embed has been generated")
    
    # Send message to Discord channel
    await channel.send(embeds=[auc_score_embed, auc_evolution_embed], files=[auc_file, auc_evolution_file])
    logger.success("Report has been sent to Discord")
    

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
        
        prediction_buffer.append(prediction_obj)
        consumer.commit()
threading.Thread(target=stream_to_buffer).start()

client.run(DISCORD_TOKEN)