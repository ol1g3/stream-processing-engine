from DataTypes import Event, Watermark, Record
from datetime import datetime
from asyncio import Queue
from WindowStrategy import WindowStrategy


class WatermarkOperator:
    def __init__(self, transactionThreshold: int, queue: Queue, windowStrategy: WindowStrategy) -> None:
        self.transactionThreshold = transactionThreshold

        self.lastSeenOn: datetime
        self.queue = queue
        self.windowStrategy = windowStrategy

    async def on_event(self, e: Record):
        if self.lastSeenOn is None:
            self.lastSeenOn = e.timestamp
        else:
            self.lastSeenOn = max(self.lastSeenOn, e.timestamp)

        if self.windowStrategy.emit_watermark():
            await self.generate_watermark(self.lastSeenOn)

    async def generate_watermark(self, value: datetime):
        watermark = Watermark(value)
        await self.queue.put(watermark)

        # send it via stream
