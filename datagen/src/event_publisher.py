import logging
import json
import dataclasses
from typing import List, Any, Optional

logger = logging.getLogger("thelook-event-publisher")

# Try to import google-cloud-pubsub, if not available, pubsub will be disabled.
try:
    from google.cloud import pubsub_v1
except ImportError:
    pubsub_v1 = None

try:
    import httpx
except ImportError:
    httpx = None

class ClickstreamEventPublisher:
    def __init__(
        self,
        project_id: Optional[str] = None,
        topic_name: Optional[str] = None,
        publish_timeout: int = 30,
        http_url: Optional[str] = None,
    ):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publish_timeout = publish_timeout
        self.http_url = http_url
        
        self.pubsub_publisher = None
        self.topic_path = None
        
        # Initialize GCP Pub/Sub if parameters are provided
        if self.project_id and self.topic_name:
            if pubsub_v1:
                try:
                    self.pubsub_publisher = pubsub_v1.PublisherClient()
                    self.topic_path = self.pubsub_publisher.topic_path(self.project_id, self.topic_name)
                    logger.info(f"Initialized GCP Pub/Sub publisher for topic: {self.topic_path}")
                except Exception as e:
                    logger.error(f"Failed to initialize GCP Pub/Sub publisher: {e}")
            else:
                logger.warning("google-cloud-pubsub library is not installed. Pub/Sub publishing will be bypassed.")

        if self.http_url:
            if httpx:
                logger.info(f"Initialized HTTP event publisher targeting: {self.http_url}")
            else:
                logger.warning("httpx library is not installed. HTTP publishing will fallback to standard urllib.")

    def publish_batch(self, events: List[Any]) -> int:
        if not events:
            return 0

        # Serialize events
        serialized_events = []
        for e in events:
            if dataclasses.is_dataclass(e):
                data = dataclasses.asdict(e)
            elif hasattr(e, "dict"):
                data = e.dict()
            elif hasattr(e, "to_dict"):
                data = e.to_dict()
            elif isinstance(e, dict):
                data = e.copy()
            else:
                try:
                    data = dict(e)
                except Exception:
                    logger.error(f"Failed to convert event of type {type(e)} to dict.")
                    continue
            
            # Convert datetime to ISO format
            for k, v in data.items():
                if hasattr(v, "isoformat"):
                    data[k] = v.isoformat()
            
            serialized_events.append(data)

        published_count = 0

        # 1. Publish via HTTP POST
        if self.http_url:
            if httpx:
                try:
                    # Send events as JSON array
                    with httpx.Client(timeout=float(self.publish_timeout)) as client:
                        response = client.post(self.http_url, json=serialized_events)
                        if response.is_success:
                            published_count = len(events)
                            logger.info(f"Successfully published {published_count} events to HTTP endpoint.")
                        else:
                            logger.error(f"HTTP publish failed with status code {response.status_code}: {response.text}")
                except Exception as e:
                    logger.error(f"Error publishing to HTTP endpoint: {e}")
            else:
                # Fallback to standard library urllib.request
                import urllib.request
                try:
                    req = urllib.request.Request(
                        self.http_url,
                        data=json.dumps(serialized_events).encode('utf-8'),
                        headers={'Content-Type': 'application/json'}
                    )
                    with urllib.request.urlopen(req, timeout=self.publish_timeout) as response:
                        if response.status in [200, 201, 202, 204]:
                            published_count = len(events)
                            logger.info(f"Successfully published {published_count} events to HTTP endpoint via urllib.")
                        else:
                            logger.error(f"urllib publish failed with status code {response.status}")
                except Exception as e:
                    logger.error(f"Error publishing to HTTP endpoint via urllib: {e}")

        # 2. Publish via GCP Pub/Sub
        if self.pubsub_publisher and self.topic_path:
            futures = []
            for data in serialized_events:
                try:
                    message_bytes = json.dumps(data).encode("utf-8")
                    future = self.pubsub_publisher.publish(self.topic_path, message_bytes)
                    futures.append(future)
                except Exception as e:
                    logger.error(f"Error preparing message for Pub/Sub: {e}")

            # Wait for all futures to complete
            pubsub_success = 0
            for future in futures:
                try:
                    future.result(timeout=self.publish_timeout)
                    pubsub_success += 1
                except Exception as e:
                    logger.error(f"Error sending message to Pub/Sub: {e}")
            
            if not self.http_url:
                published_count = pubsub_success
            else:
                published_count = max(published_count, pubsub_success)

        # 3. If no publish target is configured, log the events
        if not self.http_url and not (self.pubsub_publisher and self.topic_path):
            logger.debug(f"No publisher target configured. Logged {len(events)} events locally: {serialized_events[:2]}")
            published_count = len(events)

        return published_count
