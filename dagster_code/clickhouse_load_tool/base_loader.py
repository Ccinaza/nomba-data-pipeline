import os
import uuid
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any
import re
import json
import boto3
import clickhouse_connect
import tempfile
# from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ClickhouseBaseLoader(ABC):
    """
    Base class for loading data from various sources to Clickhouse via S3.
    
    This abstract class provides common functionality for:
    - Connecting to Clickhouse
    - Handling S3 operations
    - Schema validation
    - Incremental and full loads
    - Error handling and logging
    """
    
    def __init__(
        self,
        clickhouse_host: str,
        clickhouse_port: int,
        clickhouse_user: str,
        clickhouse_password: str,
        clickhouse_database: str,
        s3_bucket: Optional[str]=None,
        s3_access_key: Optional[str]=None,
        s3_secret_key: Optional[str]=None,
        s3_endpoint: Optional[str]=None,
        tracking_column: str = "updated_at",
        upsert_key: str = 'id',
        batch_size: int = 10000
    ):
        """
        Initialize the base loader with connection parameters.
        
        Args:
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
            upsert_key: Column that contains unique values used for upsert operations
            batch_size: Number of records to process in each batch
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Clickhouse connection parameters
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        self.clickhouse_database = clickhouse_database
        
        # S3 parameters
        self.s3_bucket = s3_bucket
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_endpoint = s3_endpoint
    
        # Load parameters
        self.tracking_column = tracking_column
        self.upsert_key = upsert_key
        self.batch_size = batch_size
        
        # Initialize connections
        self.clickhouse_client = None
        self.s3_client = None
        
    def connect_clickhouse(self) -> None:
        """Establish connection to Clickhouse."""
        try:
            self.logger.info(f"Connecting to Clickhouse at {self.clickhouse_host}:{self.clickhouse_port}")
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=self.clickhouse_user,
                password=self.clickhouse_password,
                database=self.clickhouse_database
            )
            self.logger.info("Successfully connected to Clickhouse")
        except Exception as e:
            self.logger.error(f"Failed to connect to Clickhouse: {str(e)}")
            raise
    
    
    def connect_s3(self) -> None:
        """Establish connection to S3."""
        try:
            self.logger.info("Connecting to S3")
            s3_kwargs = {
                'aws_access_key_id': self.s3_access_key,
                'aws_secret_access_key': self.s3_secret_key,
            }
            
            if self.s3_endpoint:
                s3_kwargs['endpoint_url'] = self.s3_endpoint
                
            self.s3_client = boto3.client('s3', **s3_kwargs)
            self.logger.info("Successfully connected to S3")
        except Exception as e:
            self.logger.error(f"Failed to connect to S3: {str(e)}")
            raise


    def get_clickhouse_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get the schema of a Clickhouse table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary mapping column names to their types
        """
        if not self.clickhouse_client:
            self.connect_clickhouse()
            
        try:
            query = f"DESCRIBE TABLE {table_name}"
            result = self.clickhouse_client.query(query)
            
            schema = {}
            for row in result.named_results():
                schema[row['name']] = row['type']
                
            return schema
        except Exception as e:
            self.logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            raise
    

    def upload_to_s3(self, data, s3_key: str) -> str:
        """Upload data to S3 as JSON.
        
        Args:
            data: List of dictionaries or generator yielding dictionaries
            s3_key: The S3 key to use for the uploaded file
            
        Returns:
            S3 object key (same as input s3_key)
        """
        if not self.s3_client:
            self.connect_s3()
        
        try:
            # Create a temporary file to store the JSON data
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                temp_filename = temp_file.name
                self.logger.info(f"Created temporary file {temp_filename} for S3 upload")
                
                # Write the opening bracket for a JSON array
                temp_file.write('[')
                
                # Check if data is a generator or a list
                is_generator = hasattr(data, '__next__')
                
                if is_generator:
                    # Process the generator in chunks
                    first_item = True
                    count = 0
                    
                    for item in data:
                        if not first_item:
                            temp_file.write(',')
                        else:
                            first_item = False
                        
                        json.dump(item, temp_file)
                        count += 1
                        
                        # Periodically flush to disk
                        if count % 1000 == 0:
                            temp_file.flush()
                            self.logger.info(f"Processed {count} items for S3 upload")
                    
                    self.logger.info(f"Processed a total of {count} items for S3 upload")
                else:
                    # It's a list, process it all at once
                    count = len(data)
                    if count > 0:
                        json.dump(data[0], temp_file)
                        for item in data[1:]:
                            temp_file.write(',')
                            json.dump(item, temp_file)
                    
                    self.logger.info(f"Processed {count} items for S3 upload")
                
                # Write the closing bracket for the JSON array
                temp_file.write(']')
                temp_file.flush()
            
            # Upload the file to S3
            with open(temp_filename, 'rb') as f:
                self.s3_client.upload_fileobj(f, self.s3_bucket, s3_key)
            
            self.logger.info(f"Successfully uploaded data to S3: s3://{self.s3_bucket}/{s3_key}")
            
            # Clean up the temporary file
            os.unlink(temp_filename)
            self.logger.info(f"Deleted temporary file {temp_filename}")
            
            return s3_key
        
        except Exception as e:
            self.logger.error(f"Failed to upload data to S3: {str(e)}")
            raise


    def download_from_s3(self, s3_key: str) -> List[Dict[str, Any]]: # buffer
        """
        Download data from S3.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            List of dictionaries containing the data
        """
        if not self.s3_client:
            self.connect_s3()
            
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            json_data = response['Body'].read().decode('utf-8')
            data = json.loads(json_data)
            
            self.logger.info(f"Successfully downloaded data from S3: s3://{self.s3_bucket}/{s3_key}")
            return data
        except Exception as e:
            self.logger.error(f"Failed to download data from S3: {str(e)}")
            raise
    

    def load_to_clickhouse(self, file_key: str, target_table: str, source: str = 's3', load_type: str = 'incremental', format: str = 'JSONEachRow', derived_column: str = None) -> int:
        """Load data from S3 or GCS to Clickhouse using ClickHouse's table functions.
        
        Args:
            file_key: S3 object key or GCS blob name of the conformed data
            target_table: Clickhouse table name
            source: Source type - 's3' or 'gcs'
            load_type: Type of load to perform - 'incremental', 'full', or 'snapshot'
            format: The JSON format to load to clickhouse, default is JSONEachRow
            derived_column: A column that enriches source data with ingestion date
            
        Returns:
            Number of rows loaded
        """
        if not self.clickhouse_client:
            self.connect_clickhouse()

        self._configure_clickhouse_json_settings()
        
        try:
            # NEW: If table doesn't exist, create it from file schema (fresh load)
            self.logger.info(f"Checking if table exists...")
            table_exists_result = self.table_exists(target_table)
            self.logger.info(f"table_exists() returned: {table_exists_result}")

            # NEW: If table doesn't exist, create it from file schema (fresh load)
            if not table_exists_result:
                self.logger.info(f"Table '{target_table}' does not exist. Creating from file...")
                self._create_table_from_file(file_key, target_table, source, derived_column=derived_column)
                self.logger.info(f"Table '{target_table}' created successfully.")
            else:
                self.logger.info(f"Table '{target_table}' exists. Skipping creation...")

            # Get the schema of the target table to ensure we select the right columns
            schema = self.get_clickhouse_table_schema(target_table)
            columns = list(schema.keys())

            # Verify derived_column exists in schema if specified
            if derived_column and derived_column not in columns:
                raise ValueError(
                    f"Derived column '{derived_column}' not found in table schema. "
                    f"Available columns: {columns}"
                )
            
            # Create the appropriate table function based on source
            if source.lower() == 's3':
                table_function = self._create_s3_table_function(file_key, format)
            elif source.lower() == 'gcs':
                table_function = self._create_gcs_table_function(file_key, format)
            else:
                raise ValueError(f"Unsupported source: {source}. Must be 's3' or 'gcs'")
            
            # Perform the load based on the load type
            if load_type.lower() == 'incremental':
                return self._perform_incremental_load(table_function, target_table, columns, derived_column, source)
            elif load_type.lower() == 'special':
                return self._perform_incremental_load_special(table_function, target_table, columns, derived_column, source)
            elif load_type.lower() == 'full':
                return self._perform_full_load(table_function, target_table, columns, derived_column, source)
            elif load_type.lower() == 'snapshot':
                if not derived_column:
                    raise ValueError(
                        "Snapshot loads require a 'derived_column' parameter (e.g., 'snapshot_date') "
                        "to ensure idempotency. This column will be set to today() for each run."
                    )
                return self._perform_snapshot_load(table_function, target_table, columns, derived_column, source)
            else:
                raise ValueError(f"Unsupported load type: {load_type}. Must be 'incremental', 'full', or 'snapshot'")
                
        except Exception as e:
            self.logger.error(f"Failed to load data to Clickhouse from {source.upper()}: {str(e)}")
            raise

    def _create_s3_table_function(self, s3_key: str, format: str) -> str:
        """Create S3 table function string for ClickHouse."""
        s3_access_key = self.s3_access_key
        s3_secret_key = self.s3_secret_key
        s3_bucket = self.s3_bucket
        
        # Handle custom S3 endpoint if provided
        if self.s3_endpoint:
            s3_uri = f"{self.s3_endpoint}/{self.s3_bucket}/{s3_key}"
        else:
            # Simple direct S3 URL construction
            s3_uri = f"https://{s3_bucket}.s3.eu-west-1.amazonaws.com/{s3_key}"
        
        self.logger.info(f"Using S3 URL: {s3_uri}")
        
        return f"s3('{s3_uri}','{s3_access_key}','{s3_secret_key}','{format}')"


    def _perform_incremental_load(self, table_function: str, table_name: str, columns: List[str], derived_column: str = None, source: str = None) -> int:
        """Perform an incremental load using the tracking column directly from object storage.
        
        Args:
            table_function: ClickHouse table function string
            table_name: Clickhouse table name
            columns: List of column names
            
        Returns:
            Number of rows loaded
        """

        # Create a temporary table with the same structure
        temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        create_temp_table_query = f"CREATE TABLE {temp_table} AS {table_name} ENGINE = Memory"
        # create_temp_table_query = f"CREATE TEMPORARY TABLE {temp_table} AS {table_name}" # TEMPORARY TABLE test
        self.clickhouse_client.command(create_temp_table_query)
        
        try:
            # Insert data from S3 into temporary table
            columns_str = ", ".join(columns)

            # Prepare select columns - add derived column if specified and exists in schema
            select_columns = []
            for col in columns:
                if derived_column and col == derived_column:
                    select_columns.append(f"today() as {derived_column}")
                elif col in columns:
                    select_columns.append(col)
            
            select_columns_str = ", ".join(select_columns)
            

            insert_query = f"""
            INSERT INTO {temp_table} ({columns_str})
            SELECT {select_columns_str} FROM {table_function}
            """
            self.logger.info(f"Insert query: {insert_query}")
            self.clickhouse_client.command(insert_query)
            
            # Count the number of rows inserted
            count_query = f"SELECT count() FROM {temp_table}"
            result = self.clickhouse_client.query(count_query)
            row_count = result.result_rows[0][0]
            
            if row_count == 0:
                self.logger.info("No data to load")
                return 0
            
            # Perform the upsert using the upsert key
            # First, delete records that will be updated
            if self.upsert_key:
                delete_query = f"""
                DELETE FROM {table_name} WHERE {self.upsert_key} IN 
                (SELECT {self.upsert_key} FROM {temp_table})
                """
                self.clickhouse_client.command(delete_query)
            else:
                self.logger.warning("No upsert key specified, skipping upsert")
            
            # Then insert the new/updated records
            insert_from_temp_query = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_table}
            """
            self.clickhouse_client.command(insert_from_temp_query)
            
            self.logger.info(f"Successfully loaded {row_count} rows incrementally into {table_name} from {source.upper()}")
            return row_count
            
        finally:
            # Drop the temporary table
            drop_temp_table_query = f"DROP TABLE IF EXISTS {temp_table}"
            self.clickhouse_client.command(drop_temp_table_query)

    def _perform_incremental_load_special(self, table_function: str, table_name: str, columns: List[str], derived_column: str = None, source: str = None) -> int:
        """Perform an incremental load using the tracking column directly from object storage.
        
        Args:
            table_function: ClickHouse table function string
            table_name: Clickhouse table name
            columns: List of column names
            derived_column: Column that enriches source data with ingestion date
            source: Object storage system name for logging
            
        Returns:
            Number of rows loaded (after deduplication)
        """
        # Create a temporary table with the same structure
        temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        create_temp_table_query = f"CREATE TABLE {temp_table} AS {table_name} ENGINE = Memory"
        # create_temp_table_query = f"CREATE TEMPORARY TABLE {temp_table} AS {table_name}" # TEMPORARY TABLE test
        self.clickhouse_client.command(create_temp_table_query)
        
        try:
            # Insert data from S3 into temporary table
            columns_str = ", ".join(columns)
            
            # Prepare select columns - add derived column if specified and exists in schema
            select_columns = []
            for col in columns:
                if derived_column and col == derived_column:
                    select_columns.append(f"today() as {derived_column}")
                elif col in columns:
                    select_columns.append(col)
            
            select_columns_str = ", ".join(select_columns)
            
            insert_query = f"""
            INSERT INTO {temp_table} ({columns_str})
            SELECT {select_columns_str} FROM {table_function}
            """
            self.logger.info(f"Insert query: {insert_query}")
            self.clickhouse_client.command(insert_query)
            
            # Count the number of rows inserted
            count_query = f"SELECT count() FROM {temp_table}"
            result = self.clickhouse_client.query(count_query)
            temp_row_count = result.result_rows[0][0]
            
            if temp_row_count == 0:
                self.logger.info("No data to load")
                return 0
            
            # Perform the upsert using the upsert key
            # First, delete records that will be updated
            if self.upsert_key:
                delete_query = f"""
                DELETE FROM {table_name} WHERE {self.upsert_key} IN 
                (SELECT {self.upsert_key} FROM {temp_table})
                """
                self.clickhouse_client.command(delete_query)
            else:
                self.logger.warning("No upsert key specified, skipping upsert")
            
            # Then insert the new/updated records
            insert_from_temp_query = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_table}
            """
            self.clickhouse_client.command(insert_from_temp_query)
            self.logger.info(f"Inserted {temp_row_count} rows into {table_name}")
            
            # POST-INSERT DEDUPLICATION STEP
            final_row_count = temp_row_count
            
            # Check if we have the required columns for deduplication
            # We need both upsert_key (as main_order_key) and tracking_column for deduplication
            if self.upsert_key and self.tracking_column and self.upsert_key in columns and self.tracking_column in columns:
                self.logger.info(f"Starting post-insert deduplication: keeping latest {self.tracking_column} per {self.upsert_key}")
                
                # Check if we have duplicates to clean up
                duplicate_check_query = f"""
                SELECT count() as duplicate_groups
                FROM (
                    SELECT {self.upsert_key}, count() as cnt
                    FROM {table_name}
                    GROUP BY {self.upsert_key}
                    HAVING cnt > 1
                ) duplicates
                """
                
                result = self.clickhouse_client.query(duplicate_check_query)
                duplicate_groups = result.result_rows[0][0]
                
                if duplicate_groups > 0:
                    self.logger.info(f"Found {duplicate_groups} groups with duplicate records, cleaning up...")
                    
                    # Delete older records, keeping only the latest per upsert_key
                    dedup_delete_query = f"""
                    DELETE FROM {table_name}
                    WHERE ({self.upsert_key}, {self.tracking_column}) NOT IN (
                        SELECT {self.upsert_key}, MAX({self.tracking_column})
                        FROM {table_name}
                        GROUP BY {self.upsert_key}
                    )
                    """
                    
                    self.clickhouse_client.command(dedup_delete_query)
                    
                    # Count final records after deduplication
                    final_count_query = f"SELECT count() FROM {table_name}"
                    result = self.clickhouse_client.query(final_count_query)
                    final_row_count = result.result_rows[0][0]
                    
                    duplicates_removed = temp_row_count - final_row_count
                    self.logger.info(f"Post-insert deduplication complete: removed {duplicates_removed} duplicate records")
                    self.logger.info(f"Final state: {final_row_count} unique records in {table_name}")
                else:
                    self.logger.info("No duplicate records found, deduplication not needed")
            else:
                missing_requirements = []
                if not self.upsert_key:
                    missing_requirements.append("upsert_key not configured")
                elif self.upsert_key not in columns:
                    missing_requirements.append(f"upsert_key '{self.upsert_key}' not in table columns")
                
                if not self.tracking_column:
                    missing_requirements.append("tracking_column not configured")
                elif self.tracking_column not in columns:
                    missing_requirements.append(f"tracking_column '{self.tracking_column}' not in table columns")
                
                self.logger.info(f"Skipping post-insert deduplication: {', '.join(missing_requirements)}")
            
            source_name = source.upper() if source else "SOURCE"
            self.logger.info(f"Successfully loaded {final_row_count} rows incrementally into {table_name} from {source_name}")
            return final_row_count
            
        finally:
            # Drop the temporary table
            drop_temp_table_query = f"DROP TABLE IF EXISTS {temp_table}"
            self.clickhouse_client.command(drop_temp_table_query)


    def _perform_full_load(self, table_function: str, table_name: str, columns: List[str], derived_column: str = None,  source: str = None) -> int:
        """Perform a full load (truncate and insert) directly from S3.
        
        Args:
            s3_table_function: ClickHouse S3 table function string
            table_name: Clickhouse table name
            columns: List of column names
            
        Returns:
            Number of rows loaded
        """
        # Count the number of rows in the S3 file
        count_query = f"SELECT count() FROM {table_function}"
        result = self.clickhouse_client.query(count_query)
        row_count = result.result_rows[0][0]
        
        if row_count == 0:
            self.logger.info("No data to load")
            return 0
        
        # Truncate the table
        truncate_query = f"TRUNCATE TABLE {table_name}"
        self.clickhouse_client.command(truncate_query)
        
        # Insert the data directly from S3
        columns_str = ", ".join(columns)

        # Prepare select columns - add derived column if specified and exists in schema
        select_columns = []
        for col in columns:
            if derived_column and col == derived_column:
                select_columns.append(f"today() as {derived_column}")
            elif col in columns:
                select_columns.append(col)
        
        select_columns_str = ", ".join(select_columns)

        insert_query = f"""
        INSERT INTO {table_name} ({columns_str})
        SELECT {select_columns_str} FROM {table_function}
        """
        self.clickhouse_client.command(insert_query)
        
        self.logger.info(f"Successfully loaded {row_count} rows into {table_name} (full load) from {source.upper()}")
        return row_count


    
    def _perform_snapshot_load(self, table_function: str, table_name: str, columns: List[str], derived_column: str = None,  source: str = None) -> int:
        """Perform a snapshot load (append with date-based idempotency) directly from S3.
        
        Args:
            s3_table_function: ClickHouse S3 table function string
            table_name: Clickhouse table name
            columns: List of column names
            derived_column: Column that stores the ingestion date (typically today())
            
        Returns:
            Number of rows loaded
        """
        if not derived_column:
            self.logger.warning("Snapshot load requires a derived_column parameter to ensure idempotency")
            raise ValueError("derived_column must be specified for snapshot loads")
        
        # Create a temporary table with the same structure
        temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        create_temp_table_query = f"CREATE TABLE {temp_table} AS {table_name} ENGINE = Memory"
        self.clickhouse_client.command(create_temp_table_query)
        
        try:
            # Insert data from S3 into temporary table
            columns_str = ", ".join(columns)

            # Prepare select columns - add derived column if specified and exists in schema
            select_columns = []
            for col in columns:
                if col == derived_column:
                    select_columns.append(f"today() as {derived_column}")
                else:
                    select_columns.append(col)
            
            select_columns_str = ", ".join(select_columns)
            
            insert_query = f"""
            INSERT INTO {temp_table} ({columns_str})
            SELECT {select_columns_str} FROM {table_function}
            """
            self.logger.info(f"Insert query: {insert_query}")
            self.clickhouse_client.command(insert_query)
            
            # Count the number of rows inserted
            count_query = f"SELECT count() FROM {temp_table}"
            result = self.clickhouse_client.query(count_query)
            row_count = result.result_rows[0][0]
            
            if row_count == 0:
                self.logger.info("No data to load")
                return 0
            
            # First, delete records for today to ensure idempotency
            delete_query = f"""
            ALTER TABLE {table_name} DELETE WHERE {derived_column} = today()
            """
            self.clickhouse_client.command(delete_query)
            self.logger.info(f"Deleted existing records for today from {table_name}")
            
            # Then insert the new records
            insert_from_temp_query = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_table}
            """
            self.clickhouse_client.command(insert_from_temp_query)
            
            self.logger.info(f"Successfully loaded {row_count} rows as snapshot into {table_name} from {source.upper()}")
            return row_count
            
        finally:
            # Drop the temporary table
            drop_temp_table_query = f"DROP TABLE IF EXISTS {temp_table}"
            self.clickhouse_client.command(drop_temp_table_query)



    def get_last_loaded_value(self, table_name: str) -> Optional[str]:
        """
        Get the last loaded value of the tracking column.
        
        Args:
            table_name: Clickhouse table name
            
        Returns:
            The last loaded value or None if the table is empty
        """
        if not self.clickhouse_client:
            self.connect_clickhouse()

        # NEW: Check if table exists
        if not self.table_exists(table_name):
            self.logger.warning(f"Table {table_name} does not exist yet in ClickHouse. Skipping last value check.")
            return None  # Fresh load — no last value
            
        try:
            query = f"SELECT MAX({self.tracking_column}) as last_value FROM {table_name}"
            result = self.clickhouse_client.query(query)
            
            if result.row_count > 0:
                last_value = result.first_row[0]
                return last_value
            return None
        except Exception as e:
            self.logger.error(f"Failed to get last loaded value: {str(e)}")
            raise
    

    def close_connections(self) -> None:
        """Close all connections."""
        if self.clickhouse_client:
            try:
                self.clickhouse_client.close()
                self.logger.info("Closed Clickhouse connection")
            except Exception as e:
                self.logger.error(f"Error closing Clickhouse connection: {str(e)}")
        
        # S3 client doesn't need explicit closing
    

    @abstractmethod
    def extract_data(self, source_table: str,last_value: Optional[str] = None, source_schema:str = None) -> List[Dict[str, Any]]:
        """
        Extract data from the source.
        
        Args:
            source_table: Source table name
            last_value: Last loaded value of the tracking column for incremental loads
            
        Returns:
            List of dictionaries containing the extracted data
        """
        pass
    

    @abstractmethod
    def connect_source(self) -> None:
        """Establish connection to the source database."""
        pass
    
    
    def extract_to_storage(self, source_table: str, target_table: str, destination: str = 's3', source_schema: str = None, load_type: str = 'incremental', output_key: str = None, **kwargs) -> str:
        """Extract data from source and upload to S3 or GCS.
        
        Args:
            source_table: Source table name
            target_table: Target table name (used for storage prefix)
            destination: Destination storage - 's3' or 'gcs'
            source_schema: Source schema name
            load_type: Type of load to perform - 'incremental', 'full', or 'snapshot'
            output_key: Predefined storage key to use for output
            
        Returns:
            Storage object key of the extracted data
        """
        try:
            self.logger.info(f"Starting {load_type} extraction from {source_table} to {destination.upper()}")
            
            # Validate destination
            if destination.lower() not in ['s3', 'gcs']:
                raise ValueError(f"Unsupported destination: {destination}. Must be 's3' or 'gcs'")
            
            # Connect to source
            self.connect_source()
            
            # For incremental loads, get the last loaded value
            last_value = None
            if load_type.lower() in ['incremental', 'special']:
                self.connect_clickhouse()
                last_value = self.get_last_loaded_value(target_table)
                self.logger.info(f"Last loaded value of {self.tracking_column}: {last_value}")
            
            # Extract data from source - this returns a generator
            data_generator = self.extract_data(source_table, last_value, source_schema, **kwargs)
            
            # Upload to the specified destination
            if output_key:
                # Use provided storage key
                storage_key = output_key
            else:
                # Generate a timestamp-based storage key
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                storage_key = f"{load_type}/{source_table}_to_{target_table}_{timestamp}.json"
            
            # Upload based on destination
            if destination.lower() == 's3':
                storage_key = self.upload_to_s3(data_generator, storage_key)
                self.logger.info(f"Data successfully extracted to S3: s3://{self.s3_bucket}/{storage_key}")
            elif destination.lower() == 'gcs':
                storage_key = self.upload_to_gcs(data_generator, storage_key)
                self.logger.info(f"Data successfully extracted to GCS: gs://{self.gcs_bucket}/{storage_key}")
            
            return storage_key
            
        except Exception as e:
            self.logger.error(f"Error during data extraction to {destination.upper()}: {str(e)}")
            raise
        finally:
            self.close_connections()

    # Keep the original method for backward compatibility
    def extract_to_s3(self, source_table: str, target_table: str, source_schema: str = None, load_type: str = 'incremental', output_s3_key: str = None, **kwargs) -> str:
        """Extract data from source and upload to S3.
        
        DEPRECATED: Use extract_to_storage() with destination='s3' instead.
        
        Args:
            source_table: Source table name
            target_table: Target table name (used for S3 prefix)
            source_schema: Source schema name
            load_type: Type of load to perform - 'incremental', 'full', or 'snapshot'
            output_s3_key: Predefined S3 key to use for output
            
        Returns:
            S3 object key of the extracted data
        """
        return self.extract_to_storage(
            source_table=source_table,
            target_table=target_table,
            destination='s3',
            source_schema=source_schema,
            load_type=load_type,
            output_key=output_s3_key,
            **kwargs
        )

    def _configure_clickhouse_json_settings(self):
        """Configure ClickHouse to handle problematic JSON data"""
        try:
            # THE KEY FIX - handles mixed types in JSON fields
            self.clickhouse_client.command(
                "SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 1"
            )
            # Additional helpful settings
            self.clickhouse_client.command("SET input_format_skip_unknown_fields = 1")
            self.logger.info("Applied ClickHouse JSON settings for robust ingestion")
        except Exception as e:
            self.logger.warning(f"Could not apply ClickHouse JSON settings: {e}")

    def table_exists(self, table_name: str) -> bool:
        self._configure_clickhouse_json_settings()

        query = f"""
        SELECT count() 
        FROM system.tables 
        WHERE database = '{self.clickhouse_database}' 
          AND name = '{table_name}'
        """
        
        self.logger.info(f"Checking table existence:")
        self.logger.info(f"Database: {self.clickhouse_database}")
        self.logger.info(f"Table: {table_name}")
        self.logger.info(f"Query: {query}")
        
        result = self.clickhouse_client.query(query)
        count = result.result_rows[0][0]
        exists = count > 0
        
        self.logger.info(f"Result count: {count}")
        self.logger.info(f"Table exists: {exists}")
        
        return exists

    def _create_table_from_file(self, file_key: str, target_table: str, source: str = 's3', derived_column: str = None) -> None:
        """
        Create a ClickHouse table by inferring the schema from the uploaded file.
    
        Args:
            file_key: Path to file in S3 or GCS
            target_table: Target table name in ClickHouse
            source: 's3' or 'gcs'
        """
        try:
            self._configure_clickhouse_json_settings()

            # Step 1: Sample data from file (max 500 rows)
            if source == 's3':
                if not self.s3_client:
                    self.connect_s3()
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=file_key)
                content = response['Body'].read().decode('utf-8')
            else:  # GCS
                if not self.gcs_storage_client:
                    self.connect_to_gcs()
                bucket = self.gcs_storage_client.bucket(self.gcs_bucket)
                blob = bucket.blob(file_key)
                content = blob.download_as_text()
    
            lines = content.strip().split('\n')
            sample_data = []
            for line in lines[:1000]:
                if line.strip():
                    try:
                        parsed = json.loads(line)
                        if isinstance(parsed, dict):
                            sample_data.append(parsed)
                        elif isinstance(parsed, list):
                            sample_data.extend([item for item in parsed if isinstance(item, dict)])
                    except json.JSONDecodeError:
                        continue
    
            if not sample_data:
                raise ValueError("No valid sample data found for schema inference.")
    
            # Step 2: Infer schema from sample
            schema = {}
            all_columns = set()
            for row in sample_data:
                all_columns.update(row.keys())
    
            for col in all_columns:
                values = [row.get(col) for row in sample_data if row.get(col) is not None]
                if not values:
                    schema[col] = "Nullable(String)"
                    continue

                value_types = set()
                for val in values[:100]:  # Check first 100 values
                    if isinstance(val, dict):
                        value_types.add('object')
                    elif isinstance(val, list):
                        value_types.add('array')
                    elif isinstance(val, str):
                        value_types.add('string')
                    elif isinstance(val, bool):
                        value_types.add('bool')
                    elif isinstance(val, int):
                        value_types.add('int')
                    elif isinstance(val, float):
                        value_types.add('float')
    
                # If mixed types detected, use String
                if len(value_types) > 1:
                    self.logger.warning(f"Column '{col}' has mixed types {value_types}. Using String type.")
                    schema[col] = "String"
                    continue
    
                datetime_count = 0
                date_count = 0
                for val in values[:20]:
                    if isinstance(val, str):
                        if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', val):
                            datetime_count += 1
                        elif re.match(r'^\d{4}-\d{2}-\d{2}$', val):
                            date_count += 1
    
                total_checked = min(20, len(values))
                if total_checked and datetime_count / total_checked > 0.8:
                    schema[col] = "DateTime('UTC')"
                elif total_checked and date_count / total_checked > 0.8:
                    schema[col] = "Date"
                else:
                    sample = values[0]
                    if isinstance(sample, bool):
                        schema[col] = "Bool"
                    elif isinstance(sample, int):
                        schema[col] = "Int64"
                    elif isinstance(sample, float):
                        schema[col] = "Float64"
                    elif isinstance(sample, (dict, list)):
                        schema[col] = "String"  # Store as JSON string
                    else:
                        schema[col] = "String"

            # ✅ ADD DERIVED COLUMN TO SCHEMA
            if derived_column:
                self.logger.info(f"Adding derived column '{derived_column}' as Date type to schema")
                schema[derived_column] = "Date"

    
            # Step 3: Build CREATE TABLE DDL
            columns_ddl = [f"`{k}` {v}" for k, v in schema.items()]
            ddl = f"""
            CREATE TABLE {self.clickhouse_database}.{target_table} (
                {', '.join(columns_ddl)}
            )
            ENGINE = MergeTree()
            ORDER BY tuple()
            """
    
            # Step 4: Execute DDL
            self.clickhouse_client.command(ddl)
            # self.logger.info(f"Successfully created table {target_table} with inferred schema.")
            self.logger.info(f"Successfully created table {target_table} with inferred schema" + 
                        (f" (including derived column '{derived_column}')" if derived_column else ""))

    
        except Exception as e:
            self.logger.error(f"Failed to create table from file {file_key}: {str(e)}")
            raise
    
    
    def _generate_create_table_ddl(self, table_name: str, schema: Dict[str, str]) -> str:
        """Generate CREATE TABLE statement."""
        
        columns_ddl = []
        for column_name, column_type in schema.items():
            columns_ddl.append(f"    `{column_name}` {column_type}")
        
        # Extract the joined columns to avoid backslash in f-string
        columns_str = ',\n'.join(columns_ddl)
        
        ddl = (
            f"CREATE TABLE {self.clickhouse_database}.{table_name} (\n"
            f"{columns_str}\n"
            ") ENGINE = MergeTree()\n"
            "ORDER BY tuple()"
        )
        
        self.logger.info(f"Generated DDL: {ddl}")
        return ddl