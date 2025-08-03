from typing import Protocol, Iterable
from src.DataTypes import Event, Record
from datetime import datetime

WINDOW_TIME = 100  # microseconds


class WindowStrategy(Protocol):
    def emit_watermark(self, lastSeenOn: datetime) -> bool: ...
    def process_window(self) -> Iterable[Event]: ...
    def add_event(self, event: Record): ...


class SimpleWindow(WindowStrategy):
    def __init__(self, size) -> None:
        self.window = []
        self.maxSize = size
        self.startTimestamp = None

    def emit_watermark(self, lastSeenOn: datetime) -> bool:
        return self.maxSize <= len(self.window)

    def process_window(self):
        result = self.window.copy()
        self.window.clear()
        return result

    def add_event(self, event: Record):
        self.window.append(event)
        if self.startTimestamp == None or self.startTimestamp > event.timestamp:
            self.startTimestamp = event.timestamp


class TumblingWindow(WindowStrategy):
    def __init__(self, size) -> None:
        self.window = []
        self.maxSize = size
        self.startTimestamp = datetime.now()

    def emit_watermark(self, lastSeenOn: datetime) -> bool:
        return (lastSeenOn.microsecond - self.startTimestamp.microsecond) >= WINDOW_TIME

    def process_window(self):
        result = self.window.copy()
        self.window.clear()
        return result

    def add_event(self, event: Record):
        self.window.append(event)
        if self.startTimestamp == None or self.startTimestamp < event.timestamp:
            self.startTimestamp = event.timestamp
