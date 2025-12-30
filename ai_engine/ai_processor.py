import os
import json
import time
from dotenv import load_dotenv
from google.cloud import aiplatform
from confluent_kafka import Consumer, Producer

# --- 1. CONFIGURATION ---
load_dotenv()

# Vertex AI Initialization
aiplatform.init(project=os.environ["GCP_PROJECT_ID"], location=os.environ["GCP_REGION"])
endpoint = aiplatform.Endpoint(os.environ["GCP_ENDPOINT_ID"])

# Kafka Initialization
config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}

# Consumer for raw telemetry, Producer for AI predictions
consumer = Consumer({**config, 'group.id': 'vertex-ai-group', 'auto.offset.reset': 'latest'})
producer = Producer(config)

SOURCE_TOPIC = "telemetry_brute"
TARGET_TOPIC = "predictions_ia"

def get_vertex_prediction(sensor_id, pm25, co2):
    """ Calls your Vertex AI custom model to predict future pollution levels """
    now_ms = int(time.time() * 1000)
    instance = {
        "sensor_id": str(sensor_id), 
        "moy_co2": str(co2), 
        "moy_pm25": str(pm25), 
        "window_start": str(now_ms),
        "window_end": str(now_ms + 1000)
    }
    
    try:
        response = endpoint.predict(instances=[instance])
        prediction_raw = response.predictions[0]
        
        # Extraction logic based on model output format
        if isinstance(prediction_raw, dict):
            return float(list(prediction_raw.values())[0])
        return float(prediction_raw)
    except Exception as e:
        print(f"❌ Vertex AI Error: {e}")
        return None

# --- 2. REAL-TIME PROCESSING LOOP ---
print("🚀 Vertex AI Brain connected to Kafka stream...")
consumer.subscribe([SOURCE_TOPIC])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error(): continue

        # Parse sensor data from Kafka
        data = json.loads(msg.value().decode('utf-8'))
        print(f"📥 Data received from {data['sensor_id']} (CO2: {data['co2']})")

        # Get prediction from YOUR Vertex AI model
        prediction_value = get_vertex_prediction(data['sensor_id'], data['pm25'], data['co2'])

        if prediction_value is not None:
            # Prepare payload for Dashboard (Forecast)
            # Simulating a 5-minute forward prediction for visualization
            forecast_payload = {
                "sensor_id": data['sensor_id'],
                "co2": round(prediction_value, 2), # Value predicted by AI
                "pm25": data['pm25'], # Current PM2.5 (or another prediction)
                "timestamp": int(time.time()) + 300 # Forecast for T+5 minutes
            }

            producer.produce(TARGET_TOPIC, value=json.dumps(forecast_payload).encode('utf-8'))
            producer.flush()
            print(f"🔮 AI Prediction sent to Dashboard: {prediction_value:.2f}")

except KeyboardInterrupt:
    print("Stopping Inference Service...")
finally:
    consumer.close()