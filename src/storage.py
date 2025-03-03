"""Storage module for real-time data warehouse."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/storage.log'
)
logger = logging.getLogger(__name__)


class DataWarehouseStorage:
    """Storage service for data warehouse."""

    def __init__(
        self,
        mongodb_uri: str,
        database_name: str,
        batch_size: int = 1000
    ):
        """Initialize the storage service."""
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.batch_size = batch_size

    def store_event(
        self,
        collection: str,
        event_data: Dict[str, Any],
        event_id: Optional[str] = None
    ) -> bool:
        """
        Store a single event in MongoDB.
        
        Args:
            collection: Name of the collection
            event_data: Event data to store
            event_id: Optional event ID
            
        Returns:
            bool: True if storage was successful, False otherwise
        """
        try:
            if event_id:
                event_data['_id'] = event_id

            event_data['stored_at'] = datetime.utcnow()
            self.db[collection].insert_one(event_data)
            logger.info(f"Event stored successfully in {collection}")
            return True

        except Exception as e:
            logger.error(f"Error storing event: {str(e)}")
            return False

    def batch_store_events(
        self,
        collection: str,
        events: List[Dict[str, Any]]
    ) -> bool:
        """
        Store multiple events in MongoDB using bulk operations.
        
        Args:
            collection: Name of the collection
            events: List of events to store
            
        Returns:
            bool: True if all events were stored successfully
        """
        try:
            operations = []
            for event in events:
                event['stored_at'] = datetime.utcnow()
                if '_id' not in event:
                    operations.append(UpdateOne(
                        {'event_id': event.get('event_id')},
                        {'$set': event},
                        upsert=True
                    ))
                else:
                    operations.append(UpdateOne(
                        {'_id': event['_id']},
                        {'$set': event},
                        upsert=True
                    ))

                if len(operations) >= self.batch_size:
                    self._execute_batch(collection, operations)
                    operations = []

            if operations:
                self._execute_batch(collection, operations)

            return True

        except Exception as e:
            logger.error(f"Error in batch store: {str(e)}")
            return False

    def _execute_batch(
        self,
        collection: str,
        operations: List[UpdateOne]
    ) -> None:
        """
        Execute a batch of MongoDB operations.
        
        Args:
            collection: Name of the collection
            operations: List of MongoDB operations
        """
        try:
            result = self.db[collection].bulk_write(operations, ordered=False)
            logger.info(
                f"Batch processed: {result.modified_count} modified, "
                f"{result.upserted_count} upserted"
            )
        except BulkWriteError as bwe:
            logger.error(f"Bulk write error: {str(bwe.details)}")
            raise

    def query_events(
        self,
        collection: str,
        query: Dict[str, Any],
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query events from MongoDB.
        
        Args:
            collection: Name of the collection
            query: MongoDB query
            limit: Maximum number of results
            
        Returns:
            List of matching events
        """
        try:
            return list(
                self.db[collection].find(query).limit(limit)
            )
        except Exception as e:
            logger.error(f"Error querying events: {str(e)}")
            return []

    def create_indexes(
        self,
        collection: str,
        indexes: List[str]
    ) -> None:
        """
        Create indexes for a collection.
        
        Args:
            collection: Name of the collection
            indexes: List of field names to index
        """
        try:
            for field in indexes:
                self.db[collection].create_index(field)
            logger.info(f"Created indexes for {collection}: {indexes}")
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")
            raise
