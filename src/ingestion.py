"""Data ingestion module for real-time data warehouse."""

import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/ingestion.log'
)
logger = logging.getLogger(__name__)


class DataEvent(BaseModel):
    """Data event model for ingestion."""

    source: str = Field(..., description="Source of the data")
    event_type: str = Field(..., description="Type of the event")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    payload: Dict[str, Any] = Field(..., description="Event payload")
    event_id: Optional[str] = None

    def generate_event_id(self) -> None:
        """Generate a unique event ID using SHA-256."""
        data = f"{self.source}:{self.event_type}:{self.timestamp}:{self.payload}"
        self.event_id = hashlib.sha256(data.encode()).hexdigest()


class DataIngestionService:
    """Service for handling data ingestion."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic_prefix: str,
        group_id: str = "data_warehouse_group"
    ):
        """Initialize the data ingestion service."""
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix
        self.group_id = group_id
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def ingest_event(self, event: DataEvent) -> bool:
        """
        Ingest a data event into Kafka.
        
        Args:
            event: DataEvent object containing the event data
            
        Returns:
            bool: True if ingestion was successful, False otherwise
        """
        try:
            if not event.event_id:
                event.generate_event_id()

            topic = f"{self.topic_prefix}{event.event_type}"
            self.producer.send(
                topic,
                value=event.dict()
            )
            logger.info(f"Event {event.event_id} ingested successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting event: {str(e)}")
            return False

    def subscribe_to_events(self, event_types: list[str]) -> None:
        """
        Subscribe to specified event types.
        
        Args:
            event_types: List of event types to subscribe to
        """
        topics = [f"{self.topic_prefix}{event_type}" for event_type in event_types]
        self.consumer.subscribe(topics)
        
    def process_events(self, callback: callable) -> None:
        """
        Process incoming events with a callback function.
        
        Args:
            callback: Function to process each event
        """
        try:
            for message in self.consumer:
                try:
                    event = DataEvent(**message.value)
                    callback(event)
                except Exception as e:
                    logger.error(f"Error processing event: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in event processing loop: {str(e)}")
            raise
