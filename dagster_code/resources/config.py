import os

# MongoDB Config
MONGO_CONFIG = {
    "mongo_uri": f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/",
    "mongo_database": os.getenv("MONGO_DB"),
}

# PostgreSQL Config
POSTGRES_CONFIG = {
    "postgres_host": os.getenv("POSTGRES_HOST"),
    "postgres_port": int(os.getenv("POSTGRES_PORT", 5432)),
    "postgres_user": os.getenv("POSTGRES_USER"),
    "postgres_password": os.getenv("POSTGRES_PASSWORD"),
    "postgres_database": os.getenv("POSTGRES_DB"),
}

# ClickHouse Config
CLICKHOUSE_CONFIG = {
    "clickhouse_host": os.getenv("CLICKHOUSE_HOST"),
    "clickhouse_port": int(os.getenv("CLICKHOUSE_PORT", 9000)),
    "clickhouse_user": os.getenv("CLICKHOUSE_USER"),
    "clickhouse_password": os.getenv("CLICKHOUSE_PASSWORD"),
    "clickhouse_database": os.getenv("CLICKHOUSE_DB"),
}

# MinIO Config
MINIO_CONFIG = {
    "s3_endpoint": os.getenv("MINIO_ENDPOINT"),
    "s3_bucket": os.getenv("MINIO_BUCKET"),
    "s3_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "s3_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

