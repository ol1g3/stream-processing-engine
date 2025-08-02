from Source import Source
from DataTypes import Event, EventType
from asyncio import Queue
from datetime import datetime
from threading import Thread


class SomeSource(Source):
    async def open(self, queue: Queue) -> None:
        self.queue = queue

    async def put(self, num: int) -> None:
        for i in range(num):
            event = Event("test", EventType.INSERT, datetime.now())
            self.queue.put_nowait(event)

    async def multi_put(self, num: int) -> None:
        threads = list(map(lambda x: Thread(), range(10)))

        for i in range(len(threads)):
            threads[i] = Thread(target=lambda: self.put(num))
            threads[i].start()

        for thread in threads:
            thread.join()

    async def close(self) -> None:
        self.queue.shutdown(True)
