import os
import sys
import time
import json
import random
import asyncio
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from faker import Faker

# Ensure the datagen directory is in the path
app_dir = os.path.dirname(os.path.abspath(__file__))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

# Import models & database connectivity directly from src
from src.models import User, Event, Order, OrderItem
from src.db_writer import DataWriter
from src.config import settings

app = FastAPI(
    title=settings.app_name,
    description="HTTP streaming API for clickstream events and users",
    version=settings.app_version,
    debug=settings.debug
)

# Global Faker and user cache
fake = Faker()
users_cache = []

# Database configurations mapped from settings
DB_HOST = settings.pg_host
DB_PORT = settings.pg_port
DB_DB = settings.pg_db
DB_USER = settings.pg_user
DB_PASSWORD = settings.pg_password
DB_SCHEMA = settings.pg_schema

# ---------------------------------------------------------------------------
# Kafka Producer (initialized once at startup — production pattern)
# ---------------------------------------------------------------------------
_kafka_producer = None

def get_kafka_producer():
    """Returns the singleton Kafka producer, initializing if needed."""
    global _kafka_producer
    if _kafka_producer is None:
        from confluent_kafka import Producer
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:29092")
        _kafka_producer = Producer({
            "bootstrap.servers": kafka_servers,
            "client.id": "bff-producer",
            # Linger to batch small messages — reduces overhead without adding perceptible latency
            "linger.ms": 5,
            "batch.num.messages": 100,
        })
    return _kafka_producer


def load_users():
    """Initializes user cache by loading from DB or generating them on-the-fly."""
    global users_cache
    print("Initializing users cache...")
    try:
        writer = DataWriter(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            db_name=DB_DB,
            schema=DB_SCHEMA
        )
        # Load up to 1000 users to simulate realistic traffic from the database
        rows = writer.select("users", limit=1000)
        if rows:
            users_cache = User.from_rows(rows)
            print(f"Loaded {len(users_cache)} users from database.")
    except Exception as e:
        print(f"Could not load users from database: {e}. Generating mock users instead.")
    
    if len(users_cache) < 100:
        needed = 100 - len(users_cache)
        print(f"Generating {needed} users on the fly to fill cache...")
        users_cache.extend([User.new(fake=fake) for _ in range(needed)])
    print(f"Users cache initialized with {len(users_cache)} users.")

@app.on_event("startup")
async def startup_event():
    load_users()
    # Pre-warm Kafka producer connection
    try:
        get_kafka_producer()
        print("Kafka producer initialized successfully.")
    except Exception as e:
        print(f"Warning: Could not initialize Kafka producer at startup: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Flush any pending Kafka messages before shutdown."""
    global _kafka_producer
    if _kafka_producer is not None:
        _kafka_producer.flush(timeout=10)
        print("Kafka producer flushed on shutdown.")

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def serialize_item(item) -> str:
    """Converts a model dataclass instance to a clean JSON string."""
    data = item if isinstance(item, dict) else item.__dict__.copy()
    # Remove SQLAlchemy specific state tracking if present
    data.pop("_sa_instance_state", None)
    # Serialize datetime instances to ISO string format
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.isoformat()
    return json.dumps(data)


def _kafka_publish(topic: str, key: str, payload: dict) -> None:
    """Publish a single JSON payload to the given Kafka topic (fire-and-forget)."""
    producer = get_kafka_producer()
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
    )
    # Non-blocking poll to trigger delivery callbacks without stalling the request
    producer.poll(0)

# ---------------------------------------------------------------------------
# Utility & Health endpoints
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    return {
        "message": "TheLook eCommerce BFF — Clickstream Ingestion Gateway",
        "role": "BFF (Backend-For-Frontend): accepts HTTP events from simulators/clients and forwards to Kafka",
        "endpoints": [
            "POST /events/ingest   — ingest a batch of clickstream events (JSON array)",
            "POST /users/ingest    — ingest a batch of user registration events (JSON array)",
            "GET  /stream/events  — SSE/NDJSON stream of generated events (demo/testing)",
            "GET  /stream/users   — SSE/NDJSON stream of generated users (demo/testing)",
            "GET  /health         — health check",
            "GET  /metrics        — runtime metrics",
        ]
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/metrics")
async def metrics():
    return {
        "users_in_cache": len(users_cache),
        "timestamp": datetime.utcnow().isoformat()
    }

# ---------------------------------------------------------------------------
# BFF Ingestion endpoints  (the "production-realistic" path)
# ---------------------------------------------------------------------------

@app.post("/events/ingest", status_code=202)
async def ingest_events(events: List[dict]):
    """
    BFF ingestion endpoint for clickstream events.

    Simulates the role of an API Gateway / BFF service in production:
      - Client (browser, mobile app, simulator) sends events via HTTPS POST.
      - BFF validates schema minimally, then forwards each event to Kafka.
      - Client never touches Kafka directly (no direct TCP to broker).

    Accepts a JSON array of event objects. Returns 202 Accepted immediately
    after enqueueing to Kafka (fire-and-forget delivery semantics).
    """
    if not events:
        raise HTTPException(status_code=400, detail="Empty event batch.")
    if len(events) > 5000:
        raise HTTPException(status_code=400, detail="Batch too large. Maximum 5000 events per request.")

    events_topic = os.getenv("KAFKA_EVENTS_TOPIC", "clickstream-events")
    failed = 0

    for event in events:
        # Minimal validation: required fields
        if not event.get("id") or not event.get("session_id"):
            failed += 1
            continue

        # Normalize timestamp
        if "created_at" not in event or not event["created_at"]:
            event["created_at"] = datetime.utcnow().isoformat()

        try:
            _kafka_publish(events_topic, str(event["session_id"]), event)
        except Exception:
            failed += 1

    return {
        "accepted": len(events) - failed,
        "failed": failed,
        "topic": events_topic,
    }


@app.post("/users/ingest", status_code=202)
async def ingest_users(users: List[dict]):
    """
    BFF ingestion endpoint for new user registration events.

    Accepts a JSON array of user objects and forwards to the Kafka new-users topic.
    """
    if not users:
        raise HTTPException(status_code=400, detail="Empty user batch.")
    if len(users) > 1000:
        raise HTTPException(status_code=400, detail="Batch too large. Maximum 1000 users per request.")

    users_topic = os.getenv("KAFKA_USERS_TOPIC", "new-users")
    failed = 0

    for user in users:
        if not user.get("id"):
            failed += 1
            continue
        if "created_at" not in user or not user["created_at"]:
            user["created_at"] = datetime.utcnow().isoformat()
            user["updated_at"] = datetime.utcnow().isoformat()

        try:
            _kafka_publish(users_topic, str(user["id"]), user)
        except Exception:
            failed += 1

    return {
        "accepted": len(users) - failed,
        "failed": failed,
        "topic": users_topic,
    }

# ---------------------------------------------------------------------------
# GET Streaming endpoints (for demo / testing — generates data on-the-fly)
# ---------------------------------------------------------------------------

@app.get("/stream/events")
async def stream_events(
    rate: int = 100,
    duration: int = 60,
    count: Optional[int] = None
):
    """
    Streams Clickstream Events sequentially at the specified rate (events/second).
    Output format is NDJSON (newline-delimited JSON) for easy ingestion by NiFi/Logstash.
    """
    if rate > 10000:
        raise HTTPException(status_code=400, detail="Rate too high. Maximum rate is 10000 events/sec.")

    # Calculate inter-event sleep time
    delay = 1.0 / rate if rate > 0 else 0.01
    
    async def event_generator():
        event_buffer = []
        messages_sent = 0
        start_time = time.time()
        end_time = start_time + duration
        
        while (count is None or messages_sent < count) and time.time() < end_time:
            # Generate a new session of events when the buffer becomes empty
            if not event_buffer:
                # Weighted random selection of browsing/purchasing categories
                category = random.choices(
                    ["ghost", "user_browse", "purchase", "cancel", "return"],
                    weights=[40, 45, 10, 2.5, 2.5]
                )[0]
                
                try:
                    if category == "ghost":
                        events = Event.new(None, None, "ghost", fake)
                    else:
                        user = random.choice(users_cache)
                        if category == "user_browse":
                            events = Event.new(user, None, "ghost", fake)
                        else:
                            # Order and Order Items matching current user
                            order = Order.new(user, fake)
                            order_item = OrderItem.new(order, fake)
                            events = Event.new(user, order_item, category, fake)
                    
                    event_buffer.extend(events)
                except Exception as e:
                    print(f"Error generating event session: {e}")
                    yield json.dumps({"error": str(e)}) + "\n"
                    await asyncio.sleep(1.0)
                    continue

            # Yield events from the session buffer
            if event_buffer:
                event = event_buffer.pop(0)
                # Normalize timestamp to the current instant for real-time streaming
                event.created_at = datetime.utcnow()
                
                yield serialize_item(event) + "\n"
                messages_sent += 1
                
                await asyncio.sleep(delay)

    return StreamingResponse(
        event_generator(),
        media_type="application/x-ndjson",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )

@app.get("/stream/users")
async def stream_users(
    rate: int = 50,
    duration: int = 60,
    count: Optional[int] = None
):
    """
    Streams newly generated User registration events at the specified rate.
    """
    if rate > 1000:
        raise HTTPException(status_code=400, detail="Rate too high. Maximum rate is 1000 users/sec.")

    delay = 1.0 / rate if rate > 0 else 0.02
    
    async def user_generator():
        messages_sent = 0
        start_time = time.time()
        end_time = start_time + duration
        
        while (count is None or messages_sent < count) and time.time() < end_time:
            try:
                user = User.new(fake=fake)
                # Mark created/updated timestamps as current time
                user.created_at = datetime.utcnow()
                user.updated_at = datetime.utcnow()
                
                yield serialize_item(user) + "\n"
                messages_sent += 1
                
                await asyncio.sleep(delay)
            except Exception as e:
                yield json.dumps({"error": str(e)}) + "\n"
                break
                
    return StreamingResponse(
        user_generator(),
        media_type="application/x-ndjson",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("stream_app:app", host=settings.host, port=settings.port, reload=settings.debug)
