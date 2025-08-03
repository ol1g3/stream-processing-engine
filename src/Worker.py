from __future__ import annotations
from typing import Protocol, Optional, Dict, Any, List, Iterable
from asyncio.queues import Queue
from abc import ABC, abstractmethod
from src.DataTypes import Record, Watermark, Event
from src.Storage import Storage
from src.WindowStrategy import WindowStrategy
from src.SomeSource import SomeSource
import asyncio
import logging

MAX_QUEUE_SIZE = 1000


class Worker(ABC):
    id = 0

    def __init__(self, inputQueue: 'Queue[Record]', outputQueues: 'List[Queue[Record]]', maxSize: int) -> None:
        self.inputQueue = inputQueue
        self.outputQueues = outputQueues
        self.maxSize = maxSize
        self.__class__.id += 1
        self.num_elements = 0

    async def run(self) -> None: ...
    async def process(self, record: Record) -> Optional[Record]: ...
    async def emit(self, record: Record) -> None: ...
    async def emit_window(self, records: List[Event]) -> None: ...
    async def on_watermark(self, record: Watermark) -> None: ...
    async def snapshot_capture(self) -> Dict[Any, Any]: ...
    async def restore_snapshot(self, snapshot: Dict[Any, Any]) -> None: ...
    async def on_error(self, record: Record) -> None: ...
    async def close(self) -> None: ...


class KeyByWorker(Worker):
    def __init__(self, inputQueue: 'Queue[Record]', outputQueues: 'List[Queue[Record]]', maxSize: int = MAX_QUEUE_SIZE):
        super().__init__(inputQueue, outputQueues, maxSize)
        self.id = super().id

    async def run(self) -> None:
        while True:
            record = await self.inputQueue.get()
            await self.process(record)

    async def process(self, record: Record) -> Optional[Record]:
        self.num_elements += 1
        hash_value = record.keyHash() % len(self.outputQueues)
        await self.outputQueues[hash_value].put(record)
        return record

    async def on_watermark(self, record: Watermark) -> None:
        for queue in self.outputQueues:
            await queue.put(record)

    async def snapshot_capture(self) -> Dict[Any, Any]:
        return {"id": self.id,
                "num_elements": self.num_elements}

    async def restore_snapshot(self, snapshot: Dict[Any, Any]) -> None:
        if "id" not in snapshot.keys() or "num_elements" not in snapshot.keys():
            return
        id = snapshot["id"]
        num_elements = snapshot["num_elements"]

        self.id = id
        self.num_elements = num_elements

    async def on_error(self, record: Record) -> None:
        # implement different types of on_error, now its just shutting down
        await self.close()

    async def close(self):
        self.inputQueue.shutdown(immediate=True)
        # all the output queues are already down


class FilterWorker(Worker):
    def __init__(self, inputQueue: 'Queue[Record]', outputQueues: 'List[Queue[Record]]', predicate_func,
                 maxSize: int = MAX_QUEUE_SIZE):
        super().__init__(inputQueue, outputQueues, maxSize)
        self.id = super().id
        self.predicate_function = predicate_func

    async def run(self) -> None:
        while True:
            record = await self.inputQueue.get()
            await self.process(record)

    async def process(self, record: Record) -> Optional[Record]:
        if not self.predicate_function(record):
            return None

        await self.emit(record)
        return record

    async def emit(self, record: Record) -> None:
        await self.outputQueues[0].put(record)

    async def on_watermark(self, record: Watermark) -> None:
        await self.emit(record)

    async def snapshot_capture(self) -> Dict[Any, Any]:
        return {"id": self.id,
                "num_elements": self.num_elements}

    async def restore_snapshot(self, snapshot: Dict[Any, Any]) -> None:
        if "id" not in snapshot.keys() or "num_elements" not in snapshot.keys():
            return
        id = snapshot["id"]
        num_elements = snapshot["num_elements"]

        self.id = id
        self.num_elements = num_elements

    async def on_error(self, record: Record) -> None:
        # implement different types of on_error, now its just shutting down
        await self.close()

    async def close(self):
        self.inputQueue.shutdown(immediate=True)
        # all the output queues are already down


class AggregateWorker(Worker):
    def __init__(self, inputQueue: 'Queue[Record]', outputQueues: 'Queue[List[Event]]', windowStrategy: WindowStrategy,
                 maxSize: int = MAX_QUEUE_SIZE):
        super().__init__(inputQueue, [], maxSize)
        self.outputQueues = [outputQueues]
        self.windowStrategy = windowStrategy
        self.id = super().id

    async def run(self) -> None:
        while True:
            record = await self.inputQueue.get()
            await self.process(record)

    async def process(self, record: Record) -> Optional[Record]:
        if self.windowStrategy.emit_watermark(record.timestamp):
            window_data = self.windowStrategy.process_window()
            await self.emit_window(list(window_data))
        else:
            self.windowStrategy.add_event(record)

    async def emit_window(self, records: List[Event]) -> None:
        await self.outputQueues[0].put(records)

    async def snapshot_capture(self) -> Dict[Any, Any]:
        return {"id": self.id,
                "num_elements": self.num_elements}

    async def restore_snapshot(self, snapshot: Dict[Any, Any]) -> None:
        if "id" not in snapshot.keys() or "num_elements" not in snapshot.keys():
            return
        id = snapshot["id"]
        num_elements = snapshot["num_elements"]

        self.id = id
        self.num_elements = num_elements

    async def on_error(self, record: Record) -> None:
        # implement different types of on_error, now its just shutting down
        await self.close()

    async def close(self):
        self.inputQueue.shutdown(immediate=True)
        # all the output queues are already down


class SinkWorker(Worker):
    def __init__(self, inputQueue: 'Queue[List[Event]]', storage: Storage,
                 maxSize: int = MAX_QUEUE_SIZE):
        super().__init__(Queue(), [], maxSize)
        self.inputQueue = inputQueue
        self.storage = storage
        self.id = super().id

    async def run(self) -> None:
        while True:
            record = await self.inputQueue.get()
            await self.storage.save(record)

    async def close(self):
        self.inputQueue.shutdown(immediate=True)


class PipelineBuilder():
    def __init__(self, n_workers: int, keyInputQueues: 'List[Queue[Record]]', predicate_func, windowStrategy: WindowStrategy, storage: Storage) -> None:
        self.logger = logging.getLogger("pipeline.builder")
        self.logger.info(f"Building pipeline with {n_workers} workers")

        keyOutputQueues = list(map(lambda x: Queue(), range(n_workers)))
        filterOutputQueues: 'List[Queue[Record]]' = [Queue()]
        aggregateOutputQueue: Queue[List[Event]] = Queue()

        self.keyByWorkers = list(
            map(lambda x: KeyByWorker(keyInputQueues[x], keyOutputQueues), range(n_workers)))
        self.logger.info(f"Created {len(self.keyByWorkers)} KeyBy workers")

        self.filterWorkers = list(map(lambda x: FilterWorker(
            keyOutputQueues[x], filterOutputQueues, predicate_func), range(n_workers)))
        self.logger.info(f"Created {len(self.filterWorkers)} Filter workers")

        self.windowStrategy = windowStrategy
        self.aggregateWorker = list(map(lambda x: AggregateWorker(
            filterOutputQueues[0], aggregateOutputQueue, self.windowStrategy), range(1)))
        self.sinkWorker = SinkWorker(aggregateOutputQueue, storage)

        self.logger.info("Pipeline construction complete")

    async def start_workers(self):
        tasks = []

        for worker in self.keyByWorkers + self.filterWorkers + self.aggregateWorker:
            task = asyncio.create_task(worker.run())
            tasks.append(task)
            self.logger.debug(f"Started worker {worker.id}")

        sink_task = asyncio.create_task(self.sinkWorker.run())
        tasks.append(sink_task)

        return tasks
