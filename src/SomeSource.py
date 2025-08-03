from src.Source import Source
from src.DataTypes import Event, EventType
from asyncio import Queue
from datetime import datetime
from threading import Thread


class SomeSource(Source):
    def __init__(self, queue: Queue) -> None:
        self.queue = queue

    async def open(self, queue: Queue) -> None:
        pass

    async def put(self, num: int) -> None:
        for i in range(num):
            event = Event(1, "test", EventType.INSERT, datetime.now())
            await self.queue.put(event)

    async def multi_put(self, num: int) -> None:
        threads = list(map(lambda x: Thread(
            target=lambda: self.put(num)), range(10)))

        for i in range(len(threads)):
            threads[i].start()

        for thread in threads:
            thread.join()

    async def close(self) -> None:
        self.queue.shutdown(True)
