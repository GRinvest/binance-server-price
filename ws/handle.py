import asyncio
from websockets.exceptions import ConnectionClosedError
from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from starlette.datastructures import State
from typing import List
from uvicorn.main import logger

router = APIRouter()


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    @staticmethod
    async def send_personal_message(message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)


State.manager = ConnectionManager()


@router.websocket("/connect")
async def websocket_endpoint(websocket: WebSocket):
    await State.manager.connect(websocket)
    await websocket.send_json(State.klines_ma)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        State.manager.disconnect(websocket)
        logger.info(
            f"{websocket.scope['client']} - Websocket Client # [close connection]")
    except ConnectionClosedError:
        State.manager.disconnect(websocket)
        logger.info(
            f"{websocket.scope['client']} - Websocket Client # [close connection]")
