from WatermarkOperator import WatermarkOperator
from DataTypes import Event, Watermark
from asyncio.queues import Queue
from datetime import datetime
from Storage import KVStore
from typing import Protocol
from WindowStrategy import WindowStrategy

# Constants
TRANSACTION_THRESHOLD = 10000
WATERMARK_INTERVAL = 1000
LATE_EVENT_AWAIT = 100  # time to wait for late messages


class Processor:
    def __init__(self, windowStrategy: WindowStrategy):
        self.queue = Queue(2*TRANSACTION_THRESHOLD)
        self.watermarkOperator = WatermarkOperator(
            TRANSACTION_THRESHOLD, self.queue, windowStrategy)

        self.windowStrategy = windowStrategy
        self.storage = KVStore()

    async def process_window(self):
        now = datetime.now()
        while (now.microsecond + LATE_EVENT_AWAIT < datetime.now().microsecond):
            if not self.queue.empty():
                record = await self.queue.get()
                if isinstance(record, Event):
                    self.windowStrategy.add_event(record)

        self.storage.save(self.windowStrategy.process_window())

    async def process(self):
        while (True):
            record = await self.queue.get()
            self.windowStrategy.add_event(record)

            if isinstance(record, Event):
                await self.watermarkOperator.on_event(record)

            elif isinstance(record, Watermark):
                await self.process_window()
