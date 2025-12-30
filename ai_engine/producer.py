import time
import json
import random
import math
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}

producer = Producer(config)
topic = "telemetry_brute"

def delivery_report(err, msg):
    if err is not None:
        print(f" Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

counter = 0 

def run_sensor_simulator():
    global counter
    print(f" Simulator started. Sending pattern-based data to {topic}...")
    
    try:
        while True:
            
            counter += 0.1 
            
            co2_base = 600
            co2_amplitude = 200
            co2_wave = co2_amplitude * math.sin(counter)
            co2_noise = random.uniform(-5, 5) 
            co2_final = round(co2_base + co2_wave + co2_noise, 2)
            
            pm25_base = 20
            pm25_wave = 10 * math.cos(counter) 
            pm25_final = round(pm25_base + pm25_wave + random.uniform(-2, 2), 2)
            
            payload = {
                "sensor_id": "sensor_industrial_01",
                "co2": co2_final,
                "pm25": pm25_final,
                "timestamp": int(time.time(0.1))
            }

            producer.produce(
                topic,
                key="sensor_01",
                value=json.dumps(payload),
                callback=delivery_report
            )
            
            print(f" Sent: CO2={co2_final}ppm, PM2.5={pm25_final} | Pattern: {'📈 Rising' if co2_wave > 0 else '📉 Falling'}")
            
            producer.poll(0)
            time.sleep(0.1) 
    except KeyboardInterrupt:
        print("Stopping simulator...")
    finally:
        producer.flush()

if __name__ == "__main__":
    run_sensor_simulator()