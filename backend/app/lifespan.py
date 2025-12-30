import asyncio
from contextlib import asynccontextmanager
from .kafka_consumer import kafka_loop, forecast_loop, alert_loop, close_consumers

@asynccontextmanager
async def lifespan(app):
    """
    Manages the startup and shutdown of Kafka consumer tasks.
    This ensures that background data processing starts with the server.
    """
    # Initialize the 3 background tasks for real-time data
    t1 = asyncio.create_task(kafka_loop())
    t2 = asyncio.create_task(forecast_loop())
    t3 = asyncio.create_task(alert_loop())
    
    print("✅ 3 Kafka Consumers started successfully")
    
    yield  # The application runs here
    
    # Graceful shutdown: Cancel tasks when the server stops
    t1.cancel()
    t2.cancel()
    t3.cancel()
    
    # Wait for tasks to be cancelled to avoid "Task was destroyed but it is pending" errors
    await asyncio.gather(t1, t2, t3, return_exceptions=True)
    
    # Close connection to Confluent Kafka
    close_consumers()
    print("🛑 Kafka Consumers stopped")