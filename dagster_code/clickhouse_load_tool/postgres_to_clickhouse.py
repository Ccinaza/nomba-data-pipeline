from typing import Dict, Any
from .clickhouse_load_tool.postgres_loader import PostgresToClickhouseLoader

def create_loader(config: Dict[str, Any]) -> PostgresToClickhouseLoader:
    """Create and return a configured PostgreSQL to Clickhouse loader"""
    return PostgresToClickhouseLoader(
        postgres_host=config.get('postgres_host'),
        postgres_port=int(config.get('postgres_port')),
        postgres_user=config.get('postgres_user'),
        postgres_password=config.get('postgres_password'),
        postgres_database=config.get('postgres_db'),
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
        upsert_key= config.get('upsert_key', 'id'),
        batch_size=int(config.get('batch_size', 10000))
    )

# please use this function below, it is genralised
def extract_postgres_to_object_storage(config: Dict[str, Any], source_table: str, source_schema:str,target_table: str, destination: str = 's3',
                          load_type: str = 'incremental', output_key: str = None):
    """Extract data from PostgreSQL to object storage
    
    Args:
        config: Configuration dictionary with connection parameters
        source_table: Source table name in PostgreSQL
        source_schema: Source schema in PostgreSQL
        target_table: Target table name in Clickhouse
        destination: The object storage destination. Supports s3, r2 and GCS. WIP.
                     N/B: Setting this param to s3 also works for r2 but the s3_endpoint
                          must be set.
        is_incremental: Whether to perform incremental load
        output_key: Predefined S3 key to use for output
        
    Returns:
        Object key where the extracted data is stored
    """
    loader = create_loader(config)
    
    if output_key:
        # Use the predefined S3 key
        object_key = loader.extract_to_storage(
            source_table=source_table,
            target_table=target_table,
            destination=destination,
            source_schema=source_schema,
            load_type=load_type,
            output_key=output_key
        )
    else:
        # Let the loader generate an S3 key
        object_key = loader.extract_to_storage(
            source_table=source_table,
            target_table=target_table,
            destination=destination,
            source_schema=source_schema,
            load_type=load_type
        )
    
    return object_key


def load_data_to_clickhouse(config: Dict[str, Any], 
                            file_key: str, 
                            target_table: str, 
                            source: str = 's3',
                            load_type: str = 'incremental', 
                            derived_column: str = None) -> int:
    """Load data from S3 to Clickhouse.
    
    Args:
        config: Configuration dictionary with connection parameters
        s3_key: S3 key where the data is stored
        target_table: Target table name in Clickhouse
        load_type: Type of load ('incremental', 'full', 'snapshot')
        derived_column: Column for snapshot load idempotency (e.g., 'ingestion_date')
        
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
        derived_column=derived_column
    )
    
    return rows_loaded