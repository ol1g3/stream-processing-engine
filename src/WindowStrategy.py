from typing import Protocol, Iterable, Optional
from src.DataTypes import Event, Record
from datetime import datetime
from abc import ABC, abstractmethod

WINDOW_TIME = 100  # microseconds


class WindowStrategy(ABC):
    def __init__(self) -> None:
        self.window = []
        self.startTimestamp: Optional[datetime] = None

    def emit_watermark(self, lastSeenOn: datetime) -> bool: ...
    def process_window(self) -> Iterable[Event]: ...
    def add_event(self, event: Record): ...

    def window_reset(self):
        self.window = []
        self.startTimestamp = None


class SimpleWindow(WindowStrategy):
    def __init__(self, maxSize: int) -> None:
        super().__init__()
        self.maxSize = maxSize

    def emit_watermark(self, lastSeenOn: datetime) -> bool:
        return self.maxSize <= len(self.window)

    def process_window(self):
        result = self.window.copy()
        self.window_reset()
        return result

    def add_event(self, event: Record):
        self.window.append(event)
        if self.startTimestamp == None or self.startTimestamp > event.timestamp:
            self.startTimestamp = event.timestamp


class TumblingWindow(WindowStrategy):
    def __init__(self, maxSize: int) -> None:
        super().__init__()
        self.maxSize = maxSize

    def emit_watermark(self, lastSeenOn: datetime) -> bool:
        return (self.startTimestamp != None and lastSeenOn.microsecond - self.startTimestamp.microsecond) >= WINDOW_TIME

    def process_window(self):
        result = self.window.copy()
        self.window_reset()
        return result

    def add_event(self, event: Record):
        self.window.append(event)
        if self.startTimestamp == None or self.startTimestamp < event.timestamp:
            self.startTimestamp = event.timestamp
