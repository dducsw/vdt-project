import os
import sys
import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer

# Ensure the datagen directory is in the path
app_dir = os.path.dirname(os.path.abspath(__file__))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

from src.models import User, Event, Order, OrderItem
from src.config import settings

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        pass  # successfully delivered

def main():
    print("Starting Kafka clickstream producer...")
    
    # Configure Kafka producer
    kafka_conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker-1:29092'),
        'client.id': 'datagen-producer'
    }
    
    # Wait for Kafka to be ready
    producer = None
    for i in range(10):
        try:
            producer = Producer(kafka_conf)
            break
        except Exception as e:
            print(f"Waiting for Kafka, attempt {i+1}/10... Error: {e}")
            time.sleep(5)
            
    if not producer:
        print("Could not connect to Kafka. Exiting.")
        sys.exit(1)
        
    # Initialize users cache
    from faker import Faker
    fake = Faker()
    users_cache = [User.new(fake=fake) for _ in range(200)]
    
    # Topics
    events_topic = os.getenv('KAFKA_EVENTS_TOPIC', 'clickstream-events')
    users_topic = os.getenv('KAFKA_USERS_TOPIC', 'new-users')
    
    print(f"Publishing to topics: {events_topic}, {users_topic}")
    
    rate = int(os.getenv('PUBLISH_RATE_HZ', '10')) # events per second
    delay = 1.0 / rate if rate > 0 else 0.1
    
    event_buffer = []
    
    while True:
        try:
            # Generate event
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
            
            if event_buffer:
                event = event_buffer.pop(0)
                event.created_at = datetime.utcnow()
                
                # Serialize event
                event_data = event.__dict__.copy()
                event_data.pop("_sa_instance_state", None)
                for k, v in event_data.items():
                    if isinstance(v, datetime):
                        event_data[k] = v.isoformat()
                
                # Publish to Kafka
                producer.produce(
                    events_topic,
                    key=str(event.session_id),
                    value=json.dumps(event_data).encode('utf-8'),
                    callback=delivery_report
                )
                
                # Periodically generate new users and publish them
                if random.random() < 0.05:
                    new_user = User.new(fake=fake)
                    new_user.created_at = datetime.utcnow()
                    new_user.updated_at = datetime.utcnow()
                    
                    user_data = new_user.__dict__.copy()
                    user_data.pop("_sa_instance_state", None)
                    for k, v in user_data.items():
                        if isinstance(v, datetime):
                            user_data[k] = v.isoformat()
                            
                    producer.produce(
                        users_topic,
                        key=str(new_user.id),
                        value=json.dumps(user_data).encode('utf-8'),
                        callback=delivery_report
                    )
                    
                    # Update cache
                    users_cache.append(new_user)
                    if len(users_cache) > 1000:
                        users_cache.pop(0)
                
                producer.poll(0)
                time.sleep(delay)
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error in producer loop: {e}")
            time.sleep(1.0)
            
    producer.flush()

if __name__ == "__main__":
    main()
