from fastapi import WebSocket

# Listes de clients par type de flux
live_clients: set[WebSocket] = set()
forecast_clients: set[WebSocket] = set()
alert_clients: set[WebSocket] = set()

async def broadcast_live(message: str):
    dead = []
    for ws in live_clients:
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        live_clients.remove(ws)

async def broadcast_forecast(message: str):
    dead = []
    for ws in forecast_clients:
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        forecast_clients.remove(ws)

async def broadcast_alerts(message: str):
    dead = []
    for ws in alert_clients:
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        alert_clients.remove(ws)