"""In-process feed liveness and delivery counters."""

import time
from dataclasses import dataclass, field


@dataclass
class Monitor:
    timeouts: dict[str, float]
    connected: set[str] = field(default_factory=set)
    active: set[str] = field(default_factory=set)
    activity: dict[str, float] = field(default_factory=dict)
    handoff_failures: int = 0

    def alive(self, feed_id: str) -> bool:
        return (
            feed_id in self.connected
            and feed_id in self.active
            and feed_id in self.activity
            and time.monotonic() - self.activity[feed_id] <= self.timeouts[feed_id]
        )

    def touch(self, feed_id: str) -> None:
        self.active.add(feed_id)
        self.activity[feed_id] = time.monotonic()
