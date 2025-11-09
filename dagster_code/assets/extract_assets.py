from dagster import asset, AssetExecutionContext, Output
from ..clickhouse_load_tool.mongo_loader import MongoToClickhouseLoader
from ..clickhouse_load_tool.postgres_loader import PostgresToClickhouseLoader
from ..resources.config import (
    MONGO_CONFIG, 
    POSTGRES_CONFIG, 
    CLICKHOUSE_CONFIG, 
    MINIO_CONFIG
)

@asset(
    group_name="extract_load",
    description="Extract users from MongoDB (snapshot) and load to ClickHouse"
)
def raw_users(context: AssetExecutionContext) -> Output:
    """MongoDB users - Daily snapshot (no updated_at field)"""

    context.log.info("Initializing MongoDB users pipeline...")
    
    loader = MongoToClickhouseLoader(
        **MONGO_CONFIG,
        **CLICKHOUSE_CONFIG,
        **MINIO_CONFIG,
        tracking_column='_id',
        upsert_key='_Uid',
        # derived_column='snapshot_date',
        batch_size=10000
    )
    
    context.log.info("Loader initialized. Starting MongoDB users extraction (snapshot mode)...")
    
    # Extract to S3
    context.log.info("Extracting users from MongoDB to S3...")

    s3_key = loader.extract_to_s3(
        source_table='users',
        target_table='raw_users',
        load_type='snapshot'
    )
    
    context.log.info(f"Extracted to S3: {s3_key}")
    
    # Load to ClickHouse
    context.log.info("Loading users from S3 to ClickHouse...")
    rows_loaded = loader.load_to_clickhouse(
        file_key=s3_key,
        target_table='raw_users',
        source='s3',
        load_type='snapshot',
        derived_column='snapshot_date'
    )
    
    context.log.info(f"Loaded {rows_loaded:,} rows to ClickHouse")
    context.log.info("MongoDB users pipeline completed successfully!")
    
    return Output(
        value=rows_loaded,
        metadata={
            "rows_loaded": rows_loaded,
            "s3_key": s3_key,
            "load_type": "snapshot",
            "source": "MongoDB",
            "table": "raw_users"
        }
    )


@asset(
    group_name="extract_load",
    description="Extract savings plans from PostgreSQL (incremental) and load to ClickHouse"
)
def raw_plans(context: AssetExecutionContext) -> Output:
    """PostgreSQL savings_plan - Incremental CDC using updated_at"""

    context.log.info("Initializing PostgreSQL savings_plan pipeline...")
    
    loader = PostgresToClickhouseLoader(
        **POSTGRES_CONFIG,
        **CLICKHOUSE_CONFIG,
        **MINIO_CONFIG,
        tracking_column='updated_at',
        upsert_key='plan_id',
        batch_size=10000
    )
    
    context.log.info("Loader Initialized. Starting PostgreSQL savings_plan extraction (incremental mode)...")
    
    # Extract to S3
    context.log.info("Extracting savings plans from PostgreSQL to S3...")
    s3_key = loader.extract_to_s3(
        source_table='savings_plan',
        target_table='raw_plans',
        source_schema='public',
        load_type='incremental'
    )
    
    context.log.info(f"Extracted to S3: {s3_key}")
    
    # Load to ClickHouse
    context.log.info("Loading savings plans from S3 to ClickHouse...")
    rows_loaded = loader.load_to_clickhouse(
        file_key=s3_key,
        target_table='raw_plans',
        source='s3',
        load_type='incremental'
    )
    
    context.log.info(f"Loaded {rows_loaded:,} rows to ClickHouse")
    context.log.info("PostgreSQL savings_plan pipeline completed successfully!")
    
    return Output(
        value=rows_loaded,
        metadata={
            "rows_loaded": rows_loaded,
            "s3_key": s3_key,
            "load_type": "incremental",
            "source": "PostgreSQL",
            "table": "raw_plans",
            "tracking_column": "updated_at"
        }
    )


@asset(
    group_name="extract_load",
    description="Extract transactions from PostgreSQL (incremental) and load to ClickHouse"
)
def raw_savings_transactions(context: AssetExecutionContext) -> Output:
    """PostgreSQL savingsTransaction - Incremental CDC using updated_at"""

    context.log.info("Initializing PostgreSQL transactions pipeline...")
    
    loader = PostgresToClickhouseLoader(
        **POSTGRES_CONFIG,
        **CLICKHOUSE_CONFIG,
        **MINIO_CONFIG,
        tracking_column='updated_at',
        upsert_key='txn_id',
        batch_size=10000
    )
    
    context.log.info("Loader initialized. Starting PostgreSQL savingsTransaction extraction (incremental mode)...")
    
    # Get last loaded value from ClickHouse
    last_value = loader.get_last_loaded_value("raw_savings_transactions")
    context.log.info(f"Last loaded value of {loader.tracking_column}: {last_value}")

    # Extract to S3
    context.log.info("Extracting transactions from PostgreSQL to S3...")
    s3_key = loader.extract_to_s3(
        source_table='savingsTransaction',
        target_table='raw_savings_transactions',
        source_schema='public',
        load_type='incremental'
    )
    
    context.log.info(f"Extracted to S3: {s3_key}")
    
    # Load to ClickHouse
    context.log.info("Loading transactions from S3 to Clickhouse...")
    rows_loaded = loader.load_to_clickhouse(
        file_key=s3_key,
        target_table='raw_savings_transactions',
        source='s3',
        load_type='incremental'
    )
    
    context.log.info(f"Loaded {rows_loaded:,} rows to ClickHouse")
    context.log.info("PostgreSQL transactions pipeline completed successfully!")
    
    return Output(
        value=rows_loaded,
        metadata={
            "rows_loaded": rows_loaded,
            "s3_key": s3_key,
            "load_type": "incremental",
            "source": "PostgreSQL",
            "table": "raw_savings_transactions",
            "tracking_column": "updated_at",
            "last_value": str(last_value)
        }
    )