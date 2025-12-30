import os
from dotenv import load_dotenv

load_dotenv()

def read_config():
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_API_KEY"),
        "sasl.password": os.getenv("KAFKA_API_SECRET"),
        "auto.offset.reset": "latest"
    }
    return config