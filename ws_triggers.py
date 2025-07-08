import asyncio
import websockets
import json
from datetime import datetime

class WSTrigger:
    def __init__(self, uri="ws://localhost:8000/ws/trigger"):
        self.uri = uri

    async def _send(self, message):
        try:
            async with websockets.connect(self.uri) as ws:
                await ws.send(json.dumps(message))
                return True
        except Exception as e:
            print(f"WebSocket trigger error: {e}")
            return False

    async def send_notification(self, message, channel="all", category="info"):
        return await self._send({
            "type": "notification",
            "message": message,
            "channel": channel,
            "category": category,
            "timestamp": datetime.now().isoformat()
        })

    async def trigger_refresh(self, channel="all"):
        return await self._send({
            "type": "refresh",
            "channel": channel,
            "timestamp": datetime.now().isoformat()
        })

    async def send_data_update(self, data, target, channel="all"):
        return await self._send({
            "type": "data_update",
            "data": data,
            "target": target,
            "channel": channel,
            "timestamp": datetime.now().isoformat()
        })

# Synchronous wrapper
class WSTriggerSync:
    def __init__(self):
        self.trigger = WSTrigger()

    def send_notification(self, message, **kwargs):
        return asyncio.run(self.trigger.send_notification(message, **kwargs))

    def trigger_refresh(self, **kwargs):
        return asyncio.run(self.trigger.trigger_refresh(**kwargs))

    def send_data_update(self, data, target, **kwargs):
        return asyncio.run(self.trigger.send_data_update(data, target, **kwargs))

# Global instance
ws_trigger = WSTriggerSync()