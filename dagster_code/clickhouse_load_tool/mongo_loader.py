from typing import Dict, List, Optional, Any, Union, Generator, Set
import pymongo
from bson.objectid import ObjectId
from bson import json_util
from datetime import datetime, timezone
import re
import psutil
import json
from .base_loader import ClickhouseBaseLoader


class MongoToClickhouseLoader(ClickhouseBaseLoader):
    """Class for loading data from MongoDB to Clickhouse via S3."""

    def __init__(
        self,
        mongo_uri: str,
        mongo_database: str,
        clickhouse_host: str,
        clickhouse_port: int,
        clickhouse_user: str,
        clickhouse_password: str,
        clickhouse_database: str,
        s3_bucket: str,
        s3_access_key: str,
        s3_secret_key: str,
        s3_endpoint: Optional[str] = None,
        tracking_column: str = "updated_at",
        upsert_key: str = '_id',
        batch_size: int = 10000,
    ):
        """Initialize the MongoDB to Clickhouse loader.

        Args:
            mongo_uri: MongoDB connection URI
            mongo_database: MongoDB database name
            clickhouse_host: Clickhouse server hostname
            clickhouse_port: Clickhouse server port
            clickhouse_user: Clickhouse username
            clickhouse_password: Clickhouse password
            clickhouse_database: Clickhouse database name
            s3_bucket: S3 bucket name for intermediate storage
            s3_access_key: S3 access key
            s3_secret_key: S3 secret key
            s3_endpoint: Optional custom S3 endpoint (for MinIO, etc.)
            tracking_column: Column name used for incremental loads
            batch_size: Number of records to process in each batch
        """
        super().__init__(
            clickhouse_host=clickhouse_host,
            clickhouse_port=clickhouse_port,
            clickhouse_user=clickhouse_user,
            clickhouse_password=clickhouse_password,
            clickhouse_database=clickhouse_database,
            s3_bucket=s3_bucket,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_endpoint=s3_endpoint,
            tracking_column=tracking_column,
            upsert_key=upsert_key,
            batch_size=batch_size,
        )

        # MongoDB connection parameters
        self.mongo_uri = mongo_uri
        self.mongo_database = mongo_database

        # Initialize connection
        self.mongo_client = None
        self.mongo_db = None

    def connect_source(self) -> None:
        """Establish connection to MongoDB."""
        try:
            self.logger.info(f"Connecting to MongoDB at {self.mongo_uri.split('@')[1].replace('/', '')}")
            self.mongo_client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_database]
            self.logger.info("Successfully connected to MongoDB")
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def _delete_fields_from_doc(self, doc: Dict[str, Any], fields_to_delete: Set[str]) -> Dict[str, Any]:
        """Delete specified fields from a document, including nested fields.
        
        Args:
            doc: MongoDB document
            fields_to_delete: Set of field paths to delete (e.g. 'field.nested.deep')
            
        Returns:
            Document with specified fields removed
        """
        result = doc.copy()
        
        for field_path in fields_to_delete:
            parts = field_path.split('.')
            
            # Handle simple top-level field
            if len(parts) == 1:
                if parts[0] in result:
                    del result[parts[0]]
                continue
                
            # Handle nested fields
            current = result
            for i, part in enumerate(parts[:-1]):
                if part not in current or not isinstance(current[part], dict):
                    break
                if i == len(parts) - 2:  # We're at the parent of the field to delete
                    if parts[-1] in current[part]:
                        del current[part][parts[-1]]
                else:
                    current = current[part]
                    
        return result


    def _process_mongo_document(self, doc, fields_to_delete=None, flatten_nested=False):
        """Process a MongoDB document to make it compatible with Clickhouse.
        First transforms MongoDB-specific types, then serializes using json_util.
        """
        # Step 1: Delete specified fields if any
        if fields_to_delete:
            doc = self._delete_fields_from_doc(doc, fields_to_delete)
        
        # Step 2: Convert ObjectId to string
        doc = self._convert_objectid(doc)
        
        # Step 3: Convert datetime objects to formatted strings
        doc = self._convert_datetime(doc)

        # Step 3.5: Handle NaN values (add this new step)
        # doc = self._handle_nan_values(doc)
        
        # Step 4: Handle flattening if needed
        if flatten_nested:
            doc = self._flatten_document(doc)
        
        # Step 5: Serialize to JSON string using json_util
        json_str = json_util.dumps(doc)
        
        # Step 6: Remove dollar signs
        json_str = json_str.replace("$", "")
        
        # Step 7: Convert back to Python dict
        return json_util.loads(json_str)

    def _convert_objectid(self, obj):
        """Convert ObjectId to string at any nesting level."""
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_objectid(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_objectid(item) for item in obj]
        else:
            return obj



    def _convert_datetime(self, obj):
        """
        Convert datetime objects to formatted strings at any nesting level.
        Also standardize date strings that are already in the document.
        """

        if isinstance(obj, datetime):
                # Make naive datetime timezone-aware (UTC)
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, dict):
            return {k: self._convert_datetime(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_datetime(item) for item in obj]
        else:
            return obj

    
    def _flatten_document(self, doc):
        """Flatten nested document structure."""
        flattened = {}
        for key, value in doc.items():
            if isinstance(value, dict):
                for nested_key, nested_value in self._flatten_dict(value, prefix=key).items():
                    flattened[nested_key] = nested_value
            else:
                flattened[key] = value
        return flattened

    def _convert_basic_types(self, obj):
        """Convert MongoDB-specific types to serializable types."""
        # First convert ObjectId and datetime
        obj = self._convert_objectid(obj)
        obj = self._convert_datetime(obj)
        
        # Then use json_util for any remaining MongoDB-specific types
        json_str = json_util.dumps(obj)
        json_str = json_str.replace("$", "")
        return json_util.loads(json_str)



    def extract_data(
        self, 
        collection_name: str, 
        last_value: Optional[str] = None,
        source_schema: Optional[str] = None,
        **kwargs
    ) -> Generator[Dict[str, Any], None, None]:
        """Extract data from MongoDB using a generator to minimize memory usage.

        Args:
            collection_name: MongoDB collection name
            last_value: Last loaded value of the tracking column for incremental loads
            query_filter: Additional MongoDB query filter to apply
            projection: MongoDB projection to limit fields returned
            fields_to_delete: List of field paths to delete from documents
            flatten_nested: Whether to flatten nested documents
            target_table: Name of the ClickHouse table to load data into

        Returns:
            Generator yielding dictionaries containing the extracted data
        """

        query_filter = kwargs.get('query_filter')
        projection = kwargs.get('projection')
        fields_to_delete = kwargs.get('fields_to_delete')
        flatten_nested = kwargs.get('flatten_nested', False)

        if self.mongo_db is None:
            self.connect_source()

        try:
            collection = self.mongo_db[collection_name]
            
            # Build the query
            query = query_filter.copy() if query_filter else {}
            
            if last_value is not None:
                
                try:
                    iso_string = last_value.isoformat()
                    last_datetime = datetime.fromisoformat(iso_string)
                    query[self.tracking_column] = {"$gte": last_datetime}
                except ValueError:
                    # If it's not a valid datetime string, try direct comparison
                    query[self.tracking_column] = {"$gte": last_value}
            
            # Get approximate count for logging
            total_count = collection.count_documents(query)
            self.logger.info(f"Query will return approximately {total_count} documents from {collection_name}")
            
            # Execute the query
            self.logger.info(f"Executing MongoDB query on {collection_name} with filter: {query}")
            cursor = collection.find(
                query, 
                projection=projection
            ).sort([(self.tracking_column, pymongo.ASCENDING)])
            
            # Convert fields_to_delete to a set for faster lookups
            fields_to_delete_set = set(fields_to_delete) if fields_to_delete else None
            
            # Process results in batches
            batch_size = self.batch_size
            batch_num = 0
            total_docs = 0
            current_batch = []
            
            for doc in cursor:
                current_batch.append(doc)
                
                # When we reach batch size, process and yield the batch
                if len(current_batch) >= batch_size:
                    batch_num += 1
                    total_docs += len(current_batch)
                    
                    self.logger.info(f"Processing batch {batch_num} with {len(current_batch)} documents. Total so far: {total_docs}/{total_count}")
                    
                    # Process each document in the batch
                    for doc in current_batch:
                        processed_doc = self._process_mongo_document(
                            doc, 
                            fields_to_delete=fields_to_delete_set,
                            flatten_nested=flatten_nested
                        )
                        yield processed_doc
                    
                    # Log memory usage
                    try:
                        process = psutil.Process()
                        memory_info = process.memory_info()
                        self.logger.info(f"Current memory usage: {memory_info.rss / (1024 * 1024):.2f} MB")
                    except Exception as e:
                        self.logger.warning(f"Failed to get memory usage: {str(e)}")
                    
                    # Clear the batch
                    current_batch = []
            
            # Process any remaining documents
            if current_batch:
                batch_num += 1
                total_docs += len(current_batch)
                
                self.logger.info(f"Processing final batch {batch_num} with {len(current_batch)} documents. Total: {total_docs}/{total_count}")
                
                for doc in current_batch:
                    processed_doc = self._process_mongo_document(
                        doc, 
                        fields_to_delete=fields_to_delete_set,
                        flatten_nested=flatten_nested
                    )
                    yield processed_doc
            
            self.logger.info(f"Finished processing all {total_docs} documents from {collection_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to extract data from MongoDB: {str(e)}")
            raise

    def close_connections(self) -> None:
        """Close all connections."""
        super().close_connections()
        if self.mongo_client:
            try:
                self.mongo_client.close()
                self.logger.info("Closed MongoDB connection")
            except Exception as e:
                self.logger.error(f"Error closing MongoDB connection: {str(e)}")