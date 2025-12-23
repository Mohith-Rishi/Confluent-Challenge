import os
import time
import json
import random
from dotenv import load_dotenv
from confluent_kafka import Producer

print("=== TEST: SCRIPT STARTING ===")
load_dotenv()

bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER')
api_key = os.getenv('KAFKA_API_KEY')
api_secret = os.getenv('KAFKA_API_SECRET')

if not all([bootstrap_server, api_key, api_secret]):
    print(" ERROR: Missing environment variables in .env file")
    exit()

conf = {
    'bootstrap.servers': bootstrap_server,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': api_key,
    'sasl.password': api_secret,
    'client.id': 'python-producer-pollution'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f" Delivery failed: {err}")
    else:
        print(f" Message delivered to {msg.topic()} [Partition: {msg.partition()}]")

def run_sensor():
    print(f" Simulator started on server: {bootstrap_server}")
    try:
        while True:
            data = {
                "sensor_id": "sensor_01",
                "co2": random.randint(350, 900),
                "pm25": round(random.uniform(5.0, 45.0), 2),
                "timestamp": int(time.time())
            }
            
            print(f" Attempting to send CO2: {data['co2']}...")

            producer.produce(
                'telemetry_brute',
                key="sensor_01",
                value=json.dumps(data),
                callback=delivery_report
            )
            
            producer.poll(0)
            producer.flush() 
            
            time.sleep(1) 
    except KeyboardInterrupt:
        print("Simulator stopped.")

if __name__ == "__main__":
    run_sensor()