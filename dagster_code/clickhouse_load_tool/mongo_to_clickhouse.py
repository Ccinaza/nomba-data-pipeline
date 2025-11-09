from typing import Dict, Any, List, Optional
from .clickhouse_load_tool.mongo_loader import MongoToClickhouseLoader

def create_loader(config: Dict[str, Any]) -> MongoToClickhouseLoader:
    """Create and return a configured MongoDB to Clickhouse loader"""
    return MongoToClickhouseLoader(
        mongo_uri=config.get('mongo_uri'),
        mongo_database=config.get('mongo_database'),
        clickhouse_host=config.get('clickhouse_host'),
        clickhouse_port=int(config.get('clickhouse_port', 8123)),
        clickhouse_user=config.get('clickhouse_user'),
        clickhouse_password=config.get('clickhouse_password'),
        clickhouse_database=config.get('clickhouse_db', 'default'),
        s3_bucket=config.get('s3_bucket'),
        s3_access_key=config.get('s3_access_key', ''),
        s3_secret_key=config.get('s3_secret_key', ''),
        s3_endpoint=config.get('s3_endpoint'),
        tracking_column=config.get('tracking_column', 'updated_at'),
        upsert_key= config.get('upsert_key', '_id'),
        batch_size=int(config.get('batch_size', 10000))
    )


def extract_mongo_to_object_storage(
        config: Dict[str, Any], 
        source_collection: str, 
        target_table: str,
        query_filter: str = None,
        load_type: str = "incremental", 
        destination: str = 's3',
        output_key: str = None,
        projection: Optional[Dict[str, int]] = None,
        fields_to_delete: Optional[List[str]] = None,
        flatten_nested: bool = False,
    ):
    """Extract data from Mongo DB to object storage
    
    Args:
        config: Configuration dictionary with connection parameters
        source_collection: Source collectioin name in Mongo DB
        target_table: Target table name in Clickhouse
        query_filter: Filter for mongo query
        load_type: Incremantal, special, full or snapshot
        destination: The object storage destination. Supports s3, r2 and GCS. WIP.
                     N/B: Setting this param to s3 also works for r2 but the s3_endpoint
                          must be set.
        is_incremental: Whether to perform incremental load
        output_key: Predefined S3 key to use for output
        projection: Optional dictionary specifying which fields to include/exclude from MongoDB documents
        fields_to_delete: Optional list of field names to remove from the extracted documents
        flatten_nested: Boolean flag indicating whether to flatten nested document structures
        
    Returns:
        Object key where the extracted data is stored
    """
    loader = create_loader(config)


    extraction_params = {
        'projection': projection,
        'fields_to_delete': fields_to_delete,
        'flatten_nested': flatten_nested,
        'query_filter': query_filter
    }
    

    if output_key:
        # Use the predefined S3 key
        object_key = loader.extract_to_storage(
            source_table=source_collection,
            target_table=target_table,
            destination=destination,
            load_type=load_type,
            output_key=output_key,
            **extraction_params
        )
    else:
        # Let the loader generate an S3 key
        object_key = loader.extract_to_storage(
            source_table=source_collection,
            target_table=target_table,
            destination=destination,
            load_type=load_type,
            **extraction_params
        )
    
    return object_key


def extract_mongo_to_s3(
    config: Dict[str, Any], 
    source_collection: str, 
    target_table: str,
    query_filter: str = None,
    load_type: str = "incremental", 
    output_s3_key: str = None,
    projection: Optional[Dict[str, int]] = None,
    fields_to_delete: Optional[List[str]] = None,
    flatten_nested: bool = False
) -> str:
    """Extract data from MongoDB to S3
    
    Args:
        config: Configuration dictionary with connection parameters
        source_collection: Source collection name in MongoDB
        target_table: Target table name in Clickhouse
        load: Whether to perform incremental load
        output_s3_key: Predefined S3 key to use for output
        query_filter: Additional MongoDB query filter to apply
        projection: MongoDB projection to limit fields returned
        fields_to_delete: List of field paths to delete from documents
        flatten_nested: Whether to flatten nested documents
        
    Returns:
        S3 key where the extracted data is stored
    """
    loader = create_loader(config)
    
    # Add MongoDB-specific parameters to the extraction
    extraction_params = {
        'projection': projection,
        'fields_to_delete': fields_to_delete,
        'flatten_nested': flatten_nested,
        'query_filter': query_filter
    }
    
    if output_s3_key:
        # Use the predefined S3 key
        s3_key = loader.extract_to_s3(
            source_table=source_collection,
            target_table=target_table,
            load_type=load_type,
            output_s3_key=output_s3_key,
            **extraction_params
        )
    else:
        # Let the loader generate an S3 key
        s3_key = loader.extract_to_s3(
            source_table=source_collection,
            target_table=target_table,
            load_type=load_type,
            **extraction_params
        )
    
    return s3_key


def load_data_to_clickhouse(
    config: Dict[str, Any], 
    file_key: str, 
    target_table: str,
    source: str = 's3',
    load_type: str = 'incremental',
    format: str = 'JSONEachRow',
    derived_column:str = None,
) -> int:
    """Load data from S3 to Clickhouse
    
    Args:
        config: Configuration dictionary with connection parameters
        s3_key: S3 key where the conformed data is stored
        target_table: Target table name in Clickhouse
        is_incremental: Whether to perform incremental load
        
    Returns:
        Number of rows loaded into Clickhouse
    """
    if not file_key:
        return 0
        
    loader = create_loader(config)
    
    # Load the data
    rows_loaded = loader.load_to_clickhouse(
        file_key=file_key, 
        target_table=target_table,
        source=source, 
        load_type=load_type, 
        format=format, 
        derived_column=derived_column)
    
    return rows_loaded