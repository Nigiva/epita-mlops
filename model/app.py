from dotenv import load_dotenv
from loguru import logger
import os
from logger import intercept_logging
from kafka import KafkaConsumer, KafkaProducer
import time
import json
from transformers import RobertaTokenizer, RobertaForSequenceClassification, RobertaConfig


logger.info("Starting Model")

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
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
logger.info(f"Kafka broker: {KAFKA_BROKER}")
MESSAGE_KAFKA_TOPIC = os.getenv("MESSAGE_KAFKA_TOPIC", "discordmessage")
logger.info(f"Kafka topic: {MESSAGE_KAFKA_TOPIC}")
PREDICTION_KAFKA_TOPIC = os.getenv("PREDICTION_KAFKA_TOPIC", "modelprediction")
logger.info(f"Kafka topic: {PREDICTION_KAFKA_TOPIC}")
WAIT_FOR_KAFKA = int(os.getenv("WAIT_FOR_KAFKA", 10))
logger.info(f"Wait for Kafka (secondes): {WAIT_FOR_KAFKA}")

# Set up logging
logger.add(LOG_PATH, rotation="1 day", retention="1 month", level="DEBUG")
intercept_logging("kafka", logger)

# Set up Model
logger.info("Setting up model")
config = RobertaConfig.from_pretrained("SkolkovoInstitute/roberta_toxicity_classifier")
config.output_hidden_states = True
tokenizer = RobertaTokenizer.from_pretrained("SkolkovoInstitute/roberta_toxicity_classifier")
model = RobertaForSequenceClassification.from_pretrained("SkolkovoInstitute/roberta_toxicity_classifier", config=config)
logger.success("Model ready !")

# Set up Kafka
logger.info("Waiting for Kafka broker to be ready")
time.sleep(WAIT_FOR_KAFKA)

consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKER,
    group_id="prediction-consumer",
)
consumer.subscribe(topics=[MESSAGE_KAFKA_TOPIC])

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
)
logger.success("Kafka Consumer and Producer ready !")

logger.success("Starting prediction loop")
for message in consumer:
    # Decode message
    logger.debug("Received message")
    message_obj = json.loads(message.value.decode("utf-8"))
    channel_id = message_obj["channel_id"]
    message_id = message_obj["message_id"]
    message_content = message_obj["message_content"]
    
    # Predict
    logger.debug("Predicting toxicity")
    token_id_batch = tokenizer([message_content], return_tensors="pt")["input_ids"]
    prediction = model(token_id_batch)
    logger.debug("Predicted")
    
    logger.debug("Extracting prediction and sentence embedding")
    is_toxic_batch = prediction.logits.softmax(dim=1).argmax(dim=1).detach().numpy()
    is_toxic = bool(is_toxic_batch[0])
    sentence_embedding = prediction["hidden_states"][-1][0, 0, :].detach().numpy()
    prediction_obj = {
        "channel_id": message_obj["channel_id"],
        "message_id": message_obj["message_id"],
        "is_toxic": is_toxic,
        "sentence_embedding": sentence_embedding.tolist()
    }
    
    logger.debug("Sending prediction to the stream")
    prediction_str = json.dumps(prediction_obj).encode("utf-8")
    producer.send(PREDICTION_KAFKA_TOPIC, prediction_str)
    logger.success(f"Prediction sent to the stream for {message_id=} {channel_id=}")