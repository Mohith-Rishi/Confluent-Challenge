from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
from .lifespan import lifespan
from .websocket_manager import live_clients, forecast_clients, alert_clients

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def ws_live(ws: WebSocket):
    """
    WebSocket endpoint for real-time telemetry.
    """
    await ws.accept()
    live_clients.add(ws)
    try:
        while True:
            # We use a tiny sleep to keep the connection alive 
            # without waiting for client input
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        live_clients.remove(ws)

@app.websocket("/ws/forecast")
async def ws_forecast(ws: WebSocket):
    """
    WebSocket endpoint for AI forecast data.
    """
    await ws.accept()
    forecast_clients.add(ws)
    try:
        while True:
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        forecast_clients.remove(ws)

@app.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket):
    """
    WebSocket endpoint for safety alerts.
    """
    await ws.accept()
    alert_clients.add(ws)
    try:
        while True:
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        alert_clients.remove(ws)