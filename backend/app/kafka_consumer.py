import asyncio
from confluent_kafka import Consumer
from .get_config import read_config 
from .websocket_manager import broadcast_live, broadcast_forecast, broadcast_alerts

# Read the base configuration for Confluent Kafka
base_config = read_config()

def init_c(gid, topic):
    """
    Initialize a Kafka consumer with a specific group ID and subscribe to a topic.
    """
    conf = base_config.copy()
    conf["group.id"] = gid
    # 'auto.offset.reset': 'latest' ensures we get the newest data immediately
    conf["auto.offset.reset"] = "latest" 
    c = Consumer(conf)
    c.subscribe([topic])
    return c

# Initialize consumers for different data streams
live_consumer = init_c("live-group", "telemetry_brute")
forecast_consumer = init_c("forecast-group", "predictions_ia")
alert_consumer = init_c("alert-group", "alertes_pollution")

async def kafka_loop():
    """
    Real-time loop for live telemetry data.
    """
    while True:
        # poll(0) check for messages without waiting, making it much faster
        msg = await asyncio.to_thread(live_consumer.poll, 0)
        if msg and not msg.error():
<<<<<<< HEAD
            latest_payload = msg.value().decode()

        if latest_payload:
            await broadcast_live(latest_payload)
            latest_payload = None
            await asyncio.sleep(1)
        else:
            await asyncio.sleep(0.1)

=======
            # Immediate broadcast to all connected WebSocket clients
            await broadcast_live(msg.value().decode('utf-8'))
        # Tiny yield to the event loop to prevent CPU pinning while staying reactive
        await asyncio.sleep(0.1)
>>>>>>> 1188380 (Final Hackathon Submission: Integrated Gemini 2.0, Kafka Pipeline & Docker)

async def forecast_loop():
    """
    Real-time loop for AI predictions.
    """
    while True:
        msg = await asyncio.to_thread(forecast_consumer.poll, 0)
        if msg and not msg.error():
            await broadcast_forecast(msg.value().decode('utf-8'))
        await asyncio.sleep(0.1)

async def alert_loop():
    """
    Real-time loop for pollution alerts.
    """
    while True:
        msg = await asyncio.to_thread(alert_consumer.poll, 0)
        if msg and not msg.error():
            print(f"DEBUG: Alert received from Kafka -> {msg.value().decode()}")
            await broadcast_alerts(msg.value().decode('utf-8'))
        await asyncio.sleep(0.1)

def close_consumers():
    """
    Properly close all Kafka connections on shutdown.
    """
    live_consumer.close()
    forecast_consumer.close()
    alert_consumer.close()