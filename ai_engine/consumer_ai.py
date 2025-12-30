import os
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer
import vertexai
from vertexai.generative_models import GenerativeModel

# --- 1. INITIALIZATION ---
load_dotenv()

# Initialize Vertex AI with Gemini 2.0 Flash-Lite
vertexai.init(
    project=os.getenv("GCP_PROJECT_ID"), 
    location=os.getenv("GCP_REGION", "us-central1")
)

# Use Gemini 2.0 Flash-Lite for minimal latency
model = GenerativeModel("gemini-2.0-flash-lite-001")

# Kafka Configuration
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}

# Consumer Configuration
consumer_conf = {
    **kafka_config, 
    'group.id': f'gemini-2-0-hse-group-{int(time.time())}',
    'auto.offset.reset': 'latest' # Changed to latest for demo freshness
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['moyennes_finales'])

# Producer Configuration
producer = Producer(kafka_config)
TARGET_TOPIC = "alertes_pollution"

print(f"🤖 HSE Expert (Gemini 2.0 Flash-Lite) connected to {os.getenv('GCP_PROJECT_ID')}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        try:
            # Parsing Kafka data
            raw_value = msg.value()
            # Decode payload (skipping first 5 bytes for Confluent Schema Registry if needed)
            data = json.loads(raw_value[5:].decode('utf-8'))
            
            co2 = data.get('moy_co2')
            pm25 = data.get('moy_pm25')
            sensor = data.get('sensor_id', 'Unknown')
            
            # --- EXPERT HSE PROMPT ---
            prompt = (
                f"You are a Senior HSE (Health, Safety, and Environment) Engineer. "
                f"Analyze these industrial sensor readings for {sensor}: "
                f"CO2: {co2:.2f} ppm, PM2.5: {pm25:.2f} µg/m3. "
                f"1. If thresholds are exceeded (CO2 > 1000 or PM2.5 > 25), start with 'INDUSTRIAL ALERT' and give a specific safety instruction. "
                f"2. If levels are normal, give a professional brief status report and a small safety tip. "
                f"Response must be in English, professional, and max 2 sentences.")

            # Google Cloud Vertex AI Call
            response = model.generate_content(prompt)
            conseil_ia = response.text.strip()
            
            # --- PREPARING DASHBOARD PAYLOAD ---
            alert_payload = {
                "sensor_id": sensor,
                "message": conseil_ia,
                "level": "alert" if "ALERT" in conseil_ia.upper() else "info",
                "timestamp": int(time.time())
            }

            # Sending alerts to Kafka topic for dashboard
            producer.produce(TARGET_TOPIC, value=json.dumps(alert_payload).encode('utf-8'))
            producer.flush()
            
            print(f"📊 Sensor: {sensor} | CO2 {co2:.1f} | PM2.5 {pm25:.1f}")
            print(f"💬 Gemini 2.0 Advice: {conseil_ia}")
            print("-" * 50)
            
            # Slow down for demo readability
            time.sleep(10) 
            
        except Exception as e:
            print(f"❌ Error during processing: {e}")

except KeyboardInterrupt:
    print("Stopping HSE Expert...")
finally:
    consumer.close()