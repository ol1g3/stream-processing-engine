from datetime import datetime
from dataclasses import dataclass
from typing import Any, Union
from enum import Enum


class EventType(Enum):
    INSERT = 1
    DELETE = 2
    UPDATE = 3


@dataclass(frozen=True)
class Event:
    id: int
    name: str
    type: EventType
    timestamp: datetime

    def keyHash(self) -> int:
        # simple built-in hashing for now
        return hash(self.id)


@dataclass(frozen=True)
class Watermark:
    id: int
    timestamp: datetime

    def keyHash(self) -> int:
        # simple built-in hashing for now
        return hash(self.id)


Record = Union[Event, Watermark]


@dataclass(frozen=True)
class Query:
    stuff: Any
