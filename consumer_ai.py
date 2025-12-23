import os
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Consumer
import ollama

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': f'ollama-final-group-{int(time.time())}',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['moyennes_finales'])

print(" Connecting to Ollama (Phi-3)... Waiting for averages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        try:
            raw_value = msg.value()
            data = json.loads(raw_value[5:].decode('utf-8'))
            
            co2 = data.get('moy_co2')
            pm25 = data.get('moy_pm25')
            
            prompt = (f"As an industrial safety expert (HSE), analyze these averages: "
                      f"CO2={co2:.2f} ppm, PM2.5={pm25:.2f} µg/m3. "
                      f"If industrial safety thresholds are exceeded, respond with 'INDUSTRIAL ALERT' "
                      f"followed by a safety instruction (e.g., wear a mask, check ventilation). "
                      f"Otherwise, respond with 'Status: Secure Zone'.")

            response = ollama.chat(model='phi3', messages=[
                {'role': 'user', 'content': prompt},
            ])
            
            print(f" Data: CO2 {co2:.1f} | PM2.5 {pm25:.1f}")
            print(f" Phi-3 says: {response['message']['content']}")
            print("-" * 40)
            
        except Exception as e:
            print(f"Error: {e}")

except KeyboardInterrupt:
    consumer.close()