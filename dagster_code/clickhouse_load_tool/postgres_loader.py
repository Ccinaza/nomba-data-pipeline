from typing import Dict,Optional, Any, Generator
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
import psutil
from decimal import Decimal
import json

from .base_loader import ClickhouseBaseLoader

class PostgresToClickhouseLoader(ClickhouseBaseLoader):
    """
    Class for loading data from PostgreSQL to Clickhouse via S3.
    """
    
    def __init__(
        self,
        postgres_host: str,
        postgres_port: int,
        postgres_user: str,
        postgres_password: str,
        postgres_database: str,
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
        upsert_key: str = 'id',
        batch_size: int = 10000
    ):
        """
        Initialize the PostgreSQL to Clickhouse loader.
        
        Args:
            postgres_host: PostgreSQL server hostname
            postgres_port: PostgreSQL server port
            postgres_user: PostgreSQL username
            postgres_password: PostgreSQL password
            postgres_database: PostgreSQL database name
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
            batch_size=batch_size
        )
        
        # PostgreSQL connection parameters
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_database = postgres_database
        
        # Initialize connection
        self.postgres_conn = None
        
    def connect_source(self) -> None:
        """Establish connection to PostgreSQL."""
        try:
            self.logger.info(f"Connecting to PostgreSQL at {self.postgres_host}:{self.postgres_port}")
            self.postgres_conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                user=self.postgres_user,
                password=self.postgres_password,
                database=self.postgres_database
            )
            self.logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
    


    def extract_data(self, table_name: str, last_value: Optional[str] = None, source_schema: str = 'public') -> Generator[Dict[str, Any], None, None]:
        """Extract data from PostgreSQL using a generator to minimize memory usage.
        
        Args:
            source_table: PostgreSQL table name
            last_value: Last loaded value of the tracking column for incremental loads
            source_schema: PostgreSQL schema name (defaults to 'public')
            
        Returns:
            Generator yielding dictionaries containing the extracted data
        """
        if not self.postgres_conn:
            self.connect_source()
        
        try:
            # First, get the count of rows that will be returned
            with self.postgres_conn.cursor() as cursor:
                count_query = f"SELECT COUNT(*) FROM {source_schema}.{table_name}"
                count_params = []
                
                if last_value is not None:
                    count_query += f" WHERE {self.tracking_column} > %s"
                    count_params.append(last_value)
                
                self.logger.info(f"Executing count query: {count_query} with params: {count_params}")
                cursor.execute(count_query, count_params)
                total_count = cursor.fetchone()[0]
                self.logger.info(f"Query will return approximately {total_count} rows")
            
            # Use a server-side cursor to fetch data in batches
            with self.postgres_conn.cursor(name='large_result_cursor', cursor_factory=RealDictCursor) as cursor:
                # Build the query
                query = f"SELECT * FROM {source_schema}.{table_name}"
                params = []
                
                if last_value is not None:
                    query += f" WHERE {self.tracking_column} > %s"
                    params.append(last_value)
                
                # query += f" ORDER BY {self.tracking_column} ASC"
                
                # Execute the query
                self.logger.info(f"Executing query: {query} with params: {params}")
                cursor.execute(query, params)
                
                # Log that query executed successfully
                self.logger.info(f"Query executed successfully. Starting to fetch and process results...")
                
                # Process results in batches to avoid memory issues
                batch_size = self.batch_size
                batch_num = 0
                total_rows = 0
                
                # Fetch and process in smaller batches
                while True:
                    self.logger.info(f"Fetching batch {batch_num + 1} (size: {batch_size})...")
                    batch = cursor.fetchmany(batch_size)
                    
                    if not batch:
                        self.logger.info(f"No more rows to fetch. Total rows processed: {total_rows}")
                        break
                    
                    batch_num += 1
                    batch_row_count = len(batch)
                    total_rows += batch_row_count
                    
                    self.logger.info(f"Processing batch {batch_num} with {batch_row_count} rows. Total rows so far: {total_rows}")
                    
                    # Process this batch and yield each row
                    for row in batch:
                        processed_row = {}
                        for key, value in dict(row).items():
                            # Handle datetime objects
                            if isinstance(value, datetime):
                                # Convert to string in ISO format without timezone info
                                processed_row[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                                
                            elif isinstance(value, date):
                                processed_row[key] = value.strftime('%Y-%m-%d')

                            elif isinstance(value, Decimal):
                                processed_row[key] = float(value)

                            # elif isinstance(value, dict):
                            #     processed_row[key] = json.dumps(value)

                            else:
                                processed_row[key] = value
                        
                        # Yield the processed row
                        yield processed_row
                    
                    self.logger.info(f"Finished processing batch {batch_num}.")
                    
                    # Log memory usage if possible
                    try:
                        process = psutil.Process()
                        memory_info = process.memory_info()
                        self.logger.info(f"Current memory usage: {memory_info.rss / (1024 * 1024):.2f} MB")
                    except ImportError:
                        pass
                
                self.logger.info(f"Finished processing all {total_rows} rows from {table_name}")
                
        except Exception as e:
            self.logger.error(f"Failed to extract data from PostgreSQL: {str(e)}")
            raise


    def close_connections(self) -> None:
        """Close all connections."""
        super().close_connections()
        
        if self.postgres_conn:
            try:
                self.postgres_conn.close()
                self.logger.info("Closed PostgreSQL connection")
            except Exception as e:
                self.logger.error(f"Error closing PostgreSQL connection: {str(e)}")