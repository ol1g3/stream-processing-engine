from typing import Protocol, Iterable
from DataTypes import Record


class Storage(Protocol):
    def create(self) -> None: ...
    def save(self, window: Iterable[Record]) -> None: ...
    def destroy(self) -> None: ...


class KVStore(Storage):
    def create(self) -> None:
        self.storage = dict()
        self.key = 0

    def save(self, window: Iterable[Record]) -> None:
        self.storage[self.key] = window
        self.key += 1

    def destroy(self) -> None:
        self.storage = dict()
