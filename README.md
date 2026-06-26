# tiny-events

**Zero-dependency event bus for Python.**

Like Redis Pub/Sub, but in one file. No broker, no server — just `import` and emit.

## Features

| Feature | Description |
|---------|-------------|
| **Pub/Sub** | Publish and subscribe to named events |
| **Wildcards** | `user.*`, `*.created`, `*` for pattern matching |
| **Priority** | LOW, NORMAL, HIGH, CRITICAL queue levels |
| **Once** | Subscribe for exactly one event occurrence |
| **Wait For** | Block until an event matching a pattern arrives |
| **History** | Optional event history for replay/debug |
| **Thread-Safe** | Lock-protected for concurrent use |
| **Stats** | Emitted, handled, errors, subscriptions |
| **Zero Deps** | Pure Python stdlib only |

## Install

```bash
pip install tiny-events
```

Or copy `tiny_events.py` — it's a single file.

## Quick Start

```python
from tiny_events import EventBus

bus = EventBus()

# Subscribe
bus.subscribe("user.created", lambda e: print(f"User {e.data} created!"))

# Emit
bus.emit("user.created", data="alice@example.com")
# → User alice@example.com created!
```

## Wildcards

```python
bus.subscribe("user.*", on_user_event)      # user.created, user.deleted, etc.
bus.subscribe("*.created", on_any_created)  # user.created, post.created, etc.
bus.subscribe("*", catch_all)             # Everything
```

## Priority

```python
from tiny_events import Priority

bus.subscribe("alert", pager_duty, priority=Priority.CRITICAL)
bus.subscribe("alert", log_to_file, priority=Priority.LOW)
# Critical fires first, then LOW
```

## Decorator Syntax

```python
from tiny_events import subscribe, emit

@subscribe("order.placed")
def on_order(event):
    print(f"Order: {event.data}")

emit("order.placed", data={"id": 42, "total": 99.99})
```

## Wait For Events

```python
# Block until a specific event arrives
result = bus.wait_for("upload.complete", timeout=30.0)
if result:
    print(f"Upload done: {result.data}")
else:
    print("Timeout!")
```

## One-Shot Subscriptions

```python
bus.once("payment.success", lambda e: print("Paid!"))
# Auto-removes after first trigger
```

## Stop Propagation

```python
def auth_check(event):
    if not is_authorized(event.data):
        event.stop_propagation()
        raise PermissionError("Unauthorized")

bus.subscribe("admin.*", auth_check, priority=Priority.HIGH)
bus.subscribe("admin.delete", handle_delete, priority=Priority.NORMAL)
# If auth_check stops propagation, handle_delete never fires
```

## History & Replay

```python
bus = EventBus(max_history=100)

bus.emit("event.a", data=1)
bus.emit("event.b", data=2)

# Inspect recent events
recent = bus.history(limit=5)
```

## API

| Method | Description |
|--------|-------------|
| `subscribe(pattern, handler, priority=5, once=False)` | Subscribe to events |
| `emit(name, data=None, source=None)` | Emit an event |
| `once(pattern, handler)` | Subscribe once |
| `wait_for(pattern, timeout=None)` | Block for event |
| `unsubscribe(id)` | Remove subscription |
| `history(name=None, limit=10)` | Return recent events |
| `listeners(name=None)` | List active subscriptions |
| `stats()` | Return bus statistics |

## License

MIT
