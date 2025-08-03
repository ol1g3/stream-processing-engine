import asyncio
import logging
from src.Worker import PipelineBuilder
from src.WindowStrategy import SimpleWindow
from src.Storage import KVStore
from asyncio import Queue
from src.SomeSource import SomeSource

# Setup basic logging
logging.basicConfig(level=logging.INFO)


async def main():
    storage = KVStore()
    window_strategy = SimpleWindow(5)
    n_workers = 6
    source_num = 10
    generator_num = 100

    keyInputQueues = list(
        map(lambda x: Queue(), range(n_workers)))

    source_tasks = []
    for i in range(n_workers):
        task = asyncio.create_task(SomeSource(
            keyInputQueues[i]).put(generator_num))
        source_tasks.append(task)

    pipeline = PipelineBuilder(
        n_workers=n_workers,
        keyInputQueues=keyInputQueues,
        predicate_func=lambda x: True,
        windowStrategy=window_strategy,
        storage=storage
    )

    # Start workers
    worker_tasks = await pipeline.start_workers()

    # Let it run for a short time
    print("Pipeline running...")
    await asyncio.sleep(5)

    # Check results
    print(f"Storage contains: {len(storage.storage)} windows")
    for key, window in storage.storage.items():
        print(f"Window {key}: {len(window)} events")

if __name__ == "__main__":
    asyncio.run(main())
