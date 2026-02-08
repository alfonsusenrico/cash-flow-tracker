import threading
import time


class RateLimiter:
    def __init__(self) -> None:
        self._events: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def exceeded(self, key: str, limit: int, window_seconds: int) -> bool:
        now = time.time()
        cutoff = now - window_seconds
        with self._lock:
            events = self._events.get(key, [])
            events = [ts for ts in events if ts >= cutoff]
            if len(events) >= limit:
                self._events[key] = events
                return True
            events.append(now)
            if events:
                self._events[key] = events
            else:
                self._events.pop(key, None)
            return False
