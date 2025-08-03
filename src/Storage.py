from typing import Protocol, Iterable
from DataTypes import Record


class Storage(Protocol):
    def __init__(self) -> None: ...
    async def append(self, window: Iterable[Record]) -> None: ...
    async def save(self, window: Iterable[Record]) -> None: ...
    async def destroy(self) -> None: ...


class KVStore(Storage):
    async def __init__(self) -> None:
        self.storage = dict()
        self.key = 0

    async def save(self, window: Iterable[Record]) -> None:
        self.storage[self.key] = list(window)
        self.key += 1

    async def destroy(self) -> None:
        self.storage = dict()
