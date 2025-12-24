import pandas as pd
from confluent_kafka import Consumer
import json
import os
from dotenv import load_dotenv

# 1. Load configuration from your .env file
load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 'vertex_ai_collector_group',
    'auto.offset.reset': 'earliest' # Read from the beginning of the topic
}

# 2. Initialize the Consumer
consumer = Consumer(conf)
# Make sure this matches your Flink output topic name
target_topic = 'moyennes_finales'
consumer.subscribe([target_topic])

data_for_csv = []
target_count = 100 # 100 aggregated points are enough for a clean pattern

print(f"📡 Connecting to {target_topic}...")
print(f"📊 Starting collection. Need {target_count} records for Vertex AI training...")

try:
    while len(data_for_csv) < target_count:
        # Poll Kafka for new messages (timeout of 2 seconds)
        msg = consumer.poll(2.0)
        
        if msg is None:
            print("⏳ Still waiting for messages from Flink...")
            continue
        if msg.error():
            print(f"❌ Kafka Error: {msg.error()}")
            continue
            
        # 3. FIX: Handle Confluent Magic Bytes (Skip first 5 bytes)
        raw_value = msg.value()
        try:
            # Check if the message starts with the Magic Byte (0x00)
            if raw_value[0] == 0:
                # We skip the 5 bytes of the Schema Registry header
                clean_payload = raw_value[5:].decode('utf-8')
            else:
                clean_payload = raw_value.decode('utf-8')
            
            # 4. Parse JSON and add to our list
            record = json.loads(clean_payload)
            data_for_csv.append(record)
            
            if len(data_for_csv) % 10 == 0:
                print(f"📈 Progress: {len(data_for_csv)}/{target_count} messages captured.")

        except Exception as e:
            print(f"⚠️ Warning: Could not decode message. Skipping... Error: {e}")
            continue

    # 5. Save to CSV using Pandas
    print("\n✅ Collection complete!")
    df = pd.DataFrame(data_for_csv)
    output_file = 'pollution_training_data.csv'
    df.to_csv(output_file, index=False)
    print(f"📁 File '{output_file}' is ready for Vertex AI!")

except KeyboardInterrupt:
    print("\n🛑 Collection stopped by user.")
finally:
    # Always close the consumer properly
    consumer.close()