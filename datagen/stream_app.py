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

@app.get("/")
async def root():
    return {
        "message": "TheLook eCommerce Streaming API",
        "endpoints": [
            "/stream/events",
            "/stream/users",
            "/health",
            "/metrics"
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
