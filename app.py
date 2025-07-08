import streamlit as st
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
import uvicorn
import threading
import json
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import asyncio
import os
import sys

# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.subscriptions: Dict[str, List[str]] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        if client_id in self.subscriptions:
            del self.subscriptions[client_id]

    async def send_message(self, message: str, client_id: str):
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_text(message)
            except:
                self.disconnect(client_id)

    async def broadcast(self, message: str, channel: str = "all"):
        for client_id, websocket in self.active_connections.items():
            if channel == "all" or client_id in self.subscriptions.get(channel, []):
                try:
                    await websocket.send_text(message)
                except:
                    self.disconnect(client_id)

manager = ConnectionManager()

def run_websocket_server():
    websocket_app = FastAPI()
    
    # Serve static files
    websocket_app.mount("/static", StaticFiles(directory="static"), name="static")
    
    @websocket_app.websocket("/ws/{client_id}")
    async def websocket_endpoint(websocket: WebSocket, client_id: str):
        await manager.connect(websocket, client_id)
        try:
            while True:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                if message.get("type") == "navigate":
                    await manager.send_message(
                        json.dumps({
                            "type": "navigation",
                            "url": f"/{message['page'].replace(' ', '_')}",
                            "timestamp": datetime.now().isoformat()
                        }),
                        client_id
                    )
                elif message.get("type") == "subscribe":
                    channel = message["channel"]
                    if channel not in manager.subscriptions:
                        manager.subscriptions[channel] = []
                    manager.subscriptions[channel].append(client_id)
                    await manager.send_message(
                        json.dumps({
                            "type": "subscription",
                            "status": "success",
                            "channel": channel,
                            "timestamp": datetime.now().isoformat()
                        }),
                        client_id
                    )
                elif message.get("type") == "data_request":
                    await manager.send_message(
                        json.dumps({
                            "type": "data_response",
                            "data": {"example": "data"},
                            "timestamp": datetime.now().isoformat()
                        }),
                        client_id
                    )
                
        except WebSocketDisconnect:
            manager.disconnect(client_id)
        except Exception as e:
            print(f"WebSocket error: {e}")

    uvicorn.run(websocket_app, host="0.0.0.0", port=8000)

# Streamlit Configuration
st.set_page_config(
    page_title="Multipage App",
    page_icon=":guardsman:",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .report-card {
        border: 1px solid #e0e0e0;
        border-radius: 10px;
        padding: 15px 20px;
        margin-bottom: 20px;
        background-color: white;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
    }
    .report-card:hover {
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        border-color: #4285f4;
    }
    .ws-notification {
        position: fixed;
        bottom: 20px;
        right: 20px;
        padding: 10px 15px;
        background: #4285f4;
        color: white;
        border-radius: 5px;
        z-index: 1000;
        box-shadow: 0 2px 10px rgba(0,0,0,0.2);
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Start WebSocket server in background
    ws_thread = threading.Thread(target=run_websocket_server, daemon=True)
    ws_thread.start()
    
    # Your Streamlit app content
    st.title("My Streamlit App with WebSocket")
    st.write("WebSocket server is running in the background")
    
    # WebSocket client integration
    st.markdown("""
    <script src="/static/js/websocket.js"></script>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    if os.environ.get("STREAMLIT_ALREADY_RUNNING"):
        sys.exit(0)
    
    os.environ["STREAMLIT_ALREADY_RUNNING"] = "1"
    main()