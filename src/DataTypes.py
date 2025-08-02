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
    name: str
    type: EventType
    timestamp: datetime


@dataclass(frozen=True)
class Watermark:
    timestamp: datetime


Record = Union[Event, Watermark]


@dataclass(frozen=True)
class Query:
    stuff: Any
