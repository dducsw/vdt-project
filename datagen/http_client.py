"""
http_client.py — Clickstream Simulator (HTTP Client mode)

Mô phỏng hành vi client thực tế trong production:
  browser / mobile app --HTTP POST--> BFF (/events/ingest) --> Kafka

Thay vì kafka_producer.py (kết nối TCP trực tiếp vào Kafka),
script này gửi events qua HTTP tới BFF service (stream_app.py),
đúng với mô hình bảo mật production nơi Kafka không expose ra ngoài.

Cách chạy:
  BFF_URL=http://bff:8000 PUBLISH_RATE_HZ=10 python http_client.py
"""

import os
import sys
import time
import json
import random
import urllib.request
import urllib.error
from datetime import datetime

# Ensure the datagen directory is in the path
app_dir = os.path.dirname(os.path.abspath(__file__))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

from src.models import User, Event, Order, OrderItem

try:
    from faker import Faker
except ImportError:
    print("ERROR: 'faker' is required. Run: pip install faker")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BFF_URL = os.getenv("BFF_URL", "http://bff:8000").rstrip("/")
PUBLISH_RATE_HZ = int(os.getenv("PUBLISH_RATE_HZ", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))          # events per HTTP request
USER_CACHE_SIZE = int(os.getenv("USER_CACHE_SIZE", "200"))

EVENTS_INGEST_URL = f"{BFF_URL}/events/ingest"

# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def post_json(url: str, payload: list, timeout: int = 10) -> dict:
    """
    POST a JSON array to the given URL using stdlib urllib (zero extra deps).
    Returns the parsed response dict, or raises on HTTP error.
    """
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "clickstream-simulator/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def serialize_event(event) -> dict:
    """Convert Event model instance to a plain dict."""
    data = event.__dict__.copy()
    data.pop("_sa_instance_state", None)
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.isoformat()
    return data


# ---------------------------------------------------------------------------
# Wait for BFF to be ready
# ---------------------------------------------------------------------------

def wait_for_bff(max_retries: int = 20, retry_delay: float = 5.0) -> None:
    health_url = f"{BFF_URL}/health"
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(health_url, timeout=5) as resp:
                if resp.status == 200:
                    print(f"[simulator] BFF is ready at {BFF_URL}")
                    return
        except Exception as e:
            print(f"[simulator] Waiting for BFF (attempt {attempt}/{max_retries}): {e}")
            time.sleep(retry_delay)
    print(f"[simulator] ERROR: BFF not reachable after {max_retries} attempts. Exiting.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Main producer loop
# ---------------------------------------------------------------------------

def main():
    print(f"[simulator] Starting HTTP clickstream simulator")
    print(f"[simulator] Target BFF: {BFF_URL}")
    print(f"[simulator] Rate: {PUBLISH_RATE_HZ} events/s | Batch: {BATCH_SIZE} events/request")

    wait_for_bff()

    fake = Faker()
    users_cache = [User.new(fake=fake) for _ in range(USER_CACHE_SIZE)]

    delay = 1.0 / PUBLISH_RATE_HZ if PUBLISH_RATE_HZ > 0 else 0.1
    # How often to flush: send a batch after accumulating BATCH_SIZE events
    batch_interval = delay * BATCH_SIZE

    event_buffer = []       # session events waiting to be sent
    pending_events = []     # batch accumulator — flushed when full or interval elapsed
    total_sent = 0
    total_failed = 0
    batch_start = time.time()

    print("[simulator] Generating and sending events via HTTP POST to BFF...")

    while True:
        try:
            # ---- Generate one session of events when buffer is empty ----
            if not event_buffer:
                category = random.choices(
                    ["ghost", "user_browse", "purchase", "cancel", "return"],
                    weights=[40, 45, 10, 2.5, 2.5]
                )[0]

                if category == "ghost":
                    events = Event.new(None, None, "ghost", fake)
                else:
                    user = random.choice(users_cache)
                    if category == "user_browse":
                        events = Event.new(user, None, "ghost", fake)
                    else:
                        order = Order.new(user, fake)
                        order_item = OrderItem.new(order, fake)
                        events = Event.new(user, order_item, category, fake)

                event_buffer.extend(events)

            # ---- Take one event from buffer and add to pending batch ----
            if event_buffer:
                event = event_buffer.pop(0)
                event.created_at = datetime.utcnow()
                pending_events.append(serialize_event(event))   # FIX: append, not reassign

            # ---- Flush batch when full OR time interval has elapsed ----
            elapsed = time.time() - batch_start
            if len(pending_events) >= BATCH_SIZE or (pending_events and elapsed >= batch_interval):
                try:
                    resp = post_json(EVENTS_INGEST_URL, pending_events)
                    accepted = resp.get("accepted", 0)
                    failed = resp.get("failed", 0)
                    total_sent += accepted
                    total_failed += failed
                    if total_sent % 10 == 0:
                        print(f"[simulator] Sent {total_sent} events total | +{accepted} this batch | Failed: {total_failed}")
                except urllib.error.HTTPError as e:
                    print(f"[simulator] HTTP error sending events: {e.code} {e.reason}")
                except Exception as e:
                    print(f"[simulator] Error sending events: {e}")

                pending_events = []   # reset batch after flush
                batch_start = time.time()

            time.sleep(delay)

        except KeyboardInterrupt:
            print(f"\n[simulator] Stopped. Total sent: {total_sent} | Failed: {total_failed}")
            break
        except Exception as e:
            print(f"[simulator] Unexpected error: {e}")
            time.sleep(1.0)


if __name__ == "__main__":
    main()
