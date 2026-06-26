"""tiny-events — Zero-dependency event bus for Python.

Single file. No dependencies. Production-ready pub/sub.
"""

import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Any, Callable, Dict, List, Optional, Union

__version__ = "1.0.0"
__all__ = ["EventBus", "Event", "Priority", "subscribe", "emit"]


class Priority:
    LOW = 10
    NORMAL = 5
    HIGH = 1
    CRITICAL = 0


class Event:
    """Represents an event occurrence."""

    def __init__(
        self,
        name: str,
        data: Any = None,
        source: Optional[str] = None,
        priority: int = Priority.NORMAL,
        timestamp: Optional[float] = None,
    ):
        self.id = uuid.uuid4().hex[:12]
        self.name = name
        self.data = data
        self.source = source
        self.priority = priority
        self.timestamp = timestamp or time.monotonic()
        self.handled = False
        self.stopped = False

    def stop_propagation(self) -> None:
        """Stop further propagation to remaining listeners."""
        self.stopped = True

    def __repr__(self) -> str:
        return f"<Event {self.name} id={self.id}>"


class EventBus:
    """Thread-safe event bus with pub/sub, wildcards, and priority queues."""

    def __init__(self, max_history: int = 100):
        self._listeners: Dict[str, List[tuple]] = defaultdict(list)
        self._lock = threading.RLock()
        self._history: deque = deque(maxlen=max_history)
        self._running = True
        self._stats = defaultdict(int)

    def subscribe(
        self,
        event_pattern: str,
        handler: Callable[[Event], Any],
        priority: int = Priority.NORMAL,
        once: bool = False,
    ) -> str:
        """Subscribe to an event. Returns subscription ID.

        Args:
            event_pattern: Exact name or wildcard pattern (* matches anything)
            handler: Callable that receives Event
            priority: Lower = earlier (default NORMAL=5)
            once: Auto-unsubscribe after first match
        """
        sub_id = uuid.uuid4().hex[:8]
        with self._lock:
            self._listeners[event_pattern].append((priority, handler, sub_id, once))
            # Keep sorted by priority
            self._listeners[event_pattern].sort(key=lambda x: x[0])
        self._stats["subscriptions"] += 1
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        """Remove a subscription by ID. Returns True if found."""
        with self._lock:
            for pattern, listeners in self._listeners.items():
                for i, (_, _, sid, _) in enumerate(listeners):
                    if sid == sub_id:
                        listeners.pop(i)
                        self._stats["unsubscriptions"] += 1
                        return True
        return False

    def emit(self, event: Union[Event, str], data: Any = None, source: Optional[str] = None) -> Event:
        """Emit an event to all matching listeners.

        Returns the Event object with .handled and .stopped status.
        """
        if isinstance(event, str):
            event = Event(name=event, data=data, source=source)

        self._history.append(event)
        self._stats["emitted"] += 1

        handlers = []
        with self._lock:
            for pattern, listeners in self._listeners.items():
                if self._match(event.name, pattern):
                    for priority, handler, sub_id, once in listeners:
                        handlers.append((handler, sub_id, once, pattern))

        for handler, sub_id, once, pattern in handlers:
            if event.stopped:
                break
            try:
                handler(event)
                event.handled = True
                self._stats["handled"] += 1
            except Exception:
                self._stats["errors"] += 1
            if once:
                self.unsubscribe(sub_id)

        return event

    def _match(self, event_name: str, pattern: str) -> bool:
        """Check if event name matches pattern (supports * and ** wildcards)."""
        if pattern == event_name:
            return True
        if pattern == "*":
            return True
        if pattern.endswith("*"):
            return event_name.startswith(pattern[:-1])
        if pattern.startswith("*"):
            return event_name.endswith(pattern[1:])
        if "*" in pattern:
            # Simple glob: a.*.c
            parts = pattern.split("*")
            if len(parts) == 2:
                return event_name.startswith(parts[0]) and event_name.endswith(parts[1])
        return False

    def once(self, event_pattern: str, handler: Callable[[Event], Any], priority: int = Priority.NORMAL) -> str:
        """Subscribe for exactly one event occurrence."""
        return self.subscribe(event_pattern, handler, priority, once=True)

    def wait_for(
        self,
        event_pattern: str,
        timeout: Optional[float] = None,
        predicate: Optional[Callable[[Event], bool]] = None,
    ) -> Optional[Event]:
        """Block until an event matching pattern (and optional predicate) arrives.

        Returns the Event or None if timeout.
        """
        result: List[Event] = []
        event = threading.Event()

        def handler(e: Event):
            if predicate is None or predicate(e):
                result.append(e)
                event.set()

        self.once(event_pattern, handler)
        if event.wait(timeout=timeout):
            return result[0]
        return None

    def history(self, event_name: Optional[str] = None, limit: int = 10) -> List[Event]:
        """Return recent event history, optionally filtered by name."""
        items = list(self._history)
        if event_name:
            items = [e for e in items if e.name == event_name or self._match(e.name, event_name)]
        return items[-limit:]

    def clear_history(self) -> None:
        """Clear event history."""
        self._history.clear()

    def listeners(self, event_name: Optional[str] = None) -> List[Dict]:
        """Return listener info."""
        result = []
        with self._lock:
            for pattern, subs in self._listeners.items():
                if event_name is None or self._match(event_name, pattern):
                    for priority, _, sid, once in subs:
                        result.append({
                            "pattern": pattern,
                            "priority": priority,
                            "id": sid,
                            "once": once,
                        })
        return result

    def stats(self) -> dict:
        return dict(self._stats)

    def __repr__(self) -> str:
        count = sum(len(subs) for subs in self._listeners.values())
        return f"<EventBus listeners={count} emitted={self._stats['emitted']}>"


# Module-level convenience bus
_default_bus = EventBus()


def subscribe(event_pattern: str, handler=None, priority: int = Priority.NORMAL, once: bool = False):
    """Decorator or function to subscribe to the default event bus."""
    def decorator(fn):
        _default_bus.subscribe(event_pattern, fn, priority, once)
        return fn
    if handler is None:
        return decorator
    return decorator(handler)


def emit(event: Union[Event, str], data: Any = None, source: Optional[str] = None) -> Event:
    """Emit to the default event bus."""
    return _default_bus.emit(event, data, source)


def once(event_pattern: str, handler=None, priority: int = Priority.NORMAL):
    """Subscribe once on the default bus."""
    def decorator(fn):
        _default_bus.once(event_pattern, fn, priority)
        return fn
    if handler is None:
        return decorator
    return decorator(handler)


def wait_for(event_pattern: str, timeout: Optional[float] = None) -> Optional[Event]:
    """Wait for an event on the default bus."""
    return _default_bus.wait_for(event_pattern, timeout)
