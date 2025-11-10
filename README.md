# Nomba Data Pipeline - Technical Assessment

## Executive Summary

This project demonstrates a production-grade CDC (Change Data Capture) pipeline that extracts data from MongoDB and PostgreSQL, loads it into ClickHouse, and transforms it using dbt. The solution addresses a critical real-world challenge: implementing CDC when source systems lack proper change tracking mechanisms, while maintaining data quality, idempotency, and efficient resource utilization.

**Key Features:**
- Handles CDC from sources with and without timestamp fields
- Idempotent pipeline execution (safe to rerun without duplicates)
- Dimensional modeling with SCD Type 2 for historical tracking
- Fully orchestrated via Dagster with automated scheduling

## Quick Start

```bash
# 1. Start the pipeline
docker-compose up -d

# 2. Generate sample data
docker exec dagster_code_server python setup/generate_data.py

# or via your local terminal
python setup/generate_data.py

# 3. Open Dagster UI
open http://localhost:3001

# 4. Click "Materialize all" and watch the pipeline run
```

**Time to first results: ~10 minutes**

---

## Problem Statement & Solution Design

### Business Requirement:

Build a data pipeline that:
1. Moves data from MongoDB (users) and PostgreSQL (savings_plan, savingsTransaction) to a data warehouse of choice (ClickHouse)
2. Implements Change Data Capture to track new records, updates, and deletes
3. Handles sources without CDC-enabling fields (no updated_at, created_at)
3. Runs multiple times without creating duplicates
4. **Key constraint:** Some collections lack CDC tracking fields

The MongoDB users collection has NO change tracking fields:
```javascript
{
  "_id": ObjectId("..."),
  "_Uid": "UID00000001", // Only has primary key
  "firstName": "Ada",
  "lastName": "Obi",
  "occupation": "Engineer",
  "state": "Lagos"
  // No updated_at field
  // No created_at field
}
```
**This makes traditional incremental CDC impossible because without timestamps you cannot write:**
```sql
WHERE updated_at > '2024-01-15'  -- Field doesn't exist 
```

### **Solution: Dual CDC Strategy**

I implemented two different CDC approaches based on what each source system provides:

| Source | CDC Strategy | Why |
|--------|--------------|-----|
| **MongoDB users** | Full load + dbt snapshot | No timestamps available |
| **PostgreSQL tables** | Timestamp-based incremental | Has `updated_at` column |

### Strategy 1: MongoDB - Full Load + dbt Snapshot

**The Approach:**
1. Extract entire collection daily (150K users)
2. Load to ClickHouse `raw_users` table (TRUNCATE + INSERT)
3. dbt snapshot compares today's data vs. yesterday's
4. Automatically detects changes in `state` and `occupation` columns
5. Creates SCD Type 2 history without manual logic

```sql
-- snapshots/users_snapshot.sql
{% snapshot users_snapshot %}
{{
    config(
      target_schema='nomba',
      unique_key='_id',
      strategy='check',
      check_cols=['state', 'occupation']  -- Track these for changes
    )
}}
select * from {{ ref('stg_users') }}
{% endsnapshot %}
```

**How it works:**
- **First run**: All 150K users inserted with `dbt_valid_from`, `dbt_valid_to = NULL`
- **Second run**: dbt compares each row to previous snapshot
- **If occupation or state changed**: 
  - Old row: `dbt_valid_to` set to now, marked as historical
  - New row: Created with current values, `dbt_valid_to = NULL`
- **If unchanged**: Row stays as-is

**Result**: Full history of every occupation/state change, automatically maintained by dbt.

**Why this works:**
- **Handles missing timestamps**: Delegates change detection to transformation layer
- **Idempotent**: dbt compares state, doesn't blindly append
- **No manual SCD logic**: 10 lines of config vs. 200 lines of SQL
- **Battle-tested**: dbt snapshots used by thousands of companies

**Trade-off acknowledged:**
- Must transfer 150K rows daily even if only 100 changed
- Takes ~2 minutes vs. seconds for incremental
- **But**: Only viable option without timestamps, and MongoDB handles full scans efficiently

### Strategy 2: PostgreSQL - Timestamp-Based Incremental

**The Approach:**
PostgreSQL tables have `updated_at` maintained by triggers, enabling efficient incremental extraction.

```python
# Only extract changed records
query = f"""
    SELECT * FROM savings_transaction 
    WHERE updated_at > '{last_load_timestamp}'
"""
```

**How it works:**
1. Track last successful load timestamp in state table
2. Query only records with `updated_at > last_load_timestamp`
3. Load to ClickHouse using `ReplacingMergeTree` (handles upserts)
4. Update state table with new max timestamp

**Example:**
- **First run**: Extracts all 3M transactions
- **Second run** (only 100 new transactions): Extracts only those 100
- **Processing time**: 5 seconds instead of 5 minutes

**Why this works:**
- **Efficient**: Only processes changed data
- **Low latency**: Can run every 15 minutes
- **Scalable**: Works for billions of rows
- **Standard pattern**: Used by Fivetran, Airbyte, etc.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Source Systems                            │
├─────────────────────────────────────────────────────────────┤
│  MongoDB: 150K users (no timestamps)                        │
│  PostgreSQL: 50K plans + 3M transactions (has timestamps)   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│            Dagster Orchestration Layer                       │
├─────────────────────────────────────────────────────────────┤
│  Extract Assets:                                             │
│    • raw_users (full load)                                   │
│    • raw_savings_plan (incremental)                          │
│    • raw_savings_transaction (incremental)                   │
│                                                              │
│  dbt Assets:                                                 │
│    • Staging models                                          │
│    • Snapshots (SCD Type 2)                                  │
│    • Dimension and fact tables                               │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              ClickHouse Data Warehouse                       │
├─────────────────────────────────────────────────────────────┤
│  Raw Layer:        raw_users, raw_savings_*                 │
│  Staging Layer:    stg_users, stg_savings_*                 │
│  Snapshot Layer:   users_snapshot (SCD Type 2)              │
│  Mart Layer:       dim_users, dim_savings_plan,             │
│                    fact_savings_transactions                 │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow & Lineage

### MongoDB Users Flow
```
MongoDB users (no timestamps)
  ↓ Full daily load (Dagster asset: raw_users)
raw_users table
  ↓ Clean & standardize (dbt model: stg_users)
stg_users
  ↓ SCD Type 2 tracking (dbt snapshot: users_snapshot)
users_snapshot (tracks occupation & state changes)
  ↓ Current records only (dbt model: dim_users)
dim_users (current state dimension table)
```

### PostgreSQL Transactions Flow
```
PostgreSQL savings_transaction (has updated_at)
  ↓ Incremental CDC (Dagster asset: raw_savings_transaction)
raw_savings_transaction
  ↓ Deduplicate & clean (dbt model: stg_savings_transaction)
stg_savings_transaction
  ↓ Join dimensions (dbt model: fact_savings_transactions)
fact_savings_transactions (analytics-ready fact table)
```

## Testing the CDC Logic

**Simulation script: setup/simulate_cdc.py**
```bash
python setup/simulate_cdc.py

# On every run, this file will:
# - Update existing plans
# - Insert new plans
# - Update existing transactions
# - Insert new transactions
# - Update existing users
# - Insert new users
```

### Test 1: User Updates (MongoDB Snapshot)
```bash
# Initial load
docker exec dagster_code_server dagster asset materialize --select raw_users nomba_dbt_assets

# Verify: All users have dbt_valid_to = NULL (current)
docker exec nomba-clickhouse clickhouse-client --query "
  SELECT COUNT(*) FROM nomba.users_snapshot WHERE dbt_valid_to IS NULL;
"

# Change a user's occupation
docker exec nomba-mongodb mongosh --authenticationDatabase admin -u admin -p password nomba --eval "
  db.users.updateOne({uid: 'user_001'}, {\$set: {occupation: 'Senior Engineer'}})
"

# Reload pipeline
docker exec dagster_code_server dagster asset materialize --select raw_users nomba_dbt_assets

# Verify: user_001 now has 2 records (1 historical, 1 current)
docker exec nomba-clickhouse clickhouse-client --query "
  SELECT occupation, dbt_valid_from, dbt_valid_to 
  FROM nomba.users_snapshot 
  WHERE uid = 'user_001' 
  ORDER BY dbt_valid_from;
"
# Expected:
# occupation='Engineer'       | dbt_valid_from=2024-01-15 | dbt_valid_to=2024-01-16
# occupation='Senior Engineer'| dbt_valid_from=2024-01-16 | dbt_valid_to=NULL
```

#### Verify Incremental Loading
```bash
# Verify in ClickHouse
docker exec nomba-clickhouse clickhouse-client --query "
SELECT 
    max(updated_at) as latest_update,
    count() as total_rows
FROM nomba.raw_plans
"
```

--- 

## Key Design Decisions

### Why Not Log-Based CDC (Debezium/PeerDB)?

I evaluated log-based CDC (reading PostgreSQL WAL) but chose timestamp-based because:

| Factor | Log-Based CDC | Timestamp CDC | Decision |
|--------|--------------|---------------|----------|
| **Latency** | Real-time (<1s) | Near real-time (15m) | Timestamp sufficient for analytics |
| **Complexity** | 9 containers, 12GB RAM | Simple Python, 2GB RAM | Timestamp simpler |
| **Setup time** | 4-6 hours | 1 hour | Timestamp faster |
| **Failure modes** | Complex (Temporal, workers) | Simple (retry logic) | Timestamp easier to debug |

**When I'd choose log-based:**
- High-frequency trading systems (need <1s latency)
- Capturing intermediate states (fraud detection)
- 100K+ updates per second
- Dedicated infrastructure team

**For this use case:**
- Analytics pipeline (15-min latency acceptable)
- Only need final state per record
- Local development environment
- Focus on dbt modeling, not infrastructure

### Why dbt Snapshots Instead of Custom SCD Logic?

**Alternative considered**: Build custom SCD Type 2 in Python or SQL

**Why dbt snapshots won:**
- 10 lines of config vs. 200 lines of complex SQL
- Handles edge cases automatically (backdated changes, deletes)
- Battle-tested by thousands of companies
- Built-in auditing columns
- Easy to test and maintain

### Why Dagster over Airflow?

For this assessment:
- Native dbt integration
- Asset-centric (focuses on data, not tasks)
- Lightweight (Docker runs all components)
- Better local development experience

### Why MinIO?

**Problem:** Need staging layer for ELT  
**Alternatives:** Direct load, S3  
**Decision:** Self-hosted MinIO  
**Rationale:**
- No cloud costs
- S3-compatible API
- Works with existing Load Tool
- Reproducible locally

 ---

## Idempotency Strategy

**Requirement**: Pipeline must run multiple times without creating duplicates.

### MongoDB (Full Load)
```sql
-- Uses TRUNCATE + INSERT strategy
TRUNCATE TABLE raw_users;
INSERT INTO raw_users SELECT * FROM source;
```
Result: Always exactly 150K rows (current state)

### PostgreSQL (Incremental)
```python
# Uses ReplacingMergeTree engine
CREATE TABLE raw_savings_transaction (
    txn_id String,
    ...
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (txn_id);
```
Result: ClickHouse automatically keeps latest version per `txn_id`

### dbt Snapshots
dbt compares state, doesn't blindly append. Running twice with same data = no duplicates.

## Performance Characteristics

| Operation | Volume | Time | Notes |
|-----------|--------|------|-------|
| MongoDB full load | 150K users | ~2 min | Batched at 10K rows |
| Postgres incremental (initial) | 3M transactions | ~5 min | First load only |
| Postgres incremental (delta) | ~100 changes | ~5 sec | Typical run |
| dbt snapshot | 150K comparisons | ~30 sec | In-database |
| Full pipeline | All sources | ~5 min | Parallel execution |

---

## CI/CD Pipeline

### What It Does

The CI/CD pipeline runs on every push or pull request to main and performs **static checks** only:
```yaml
1. **Code Quality**
   - Python linting using `ruff`
   - Code formatting check using `black`
   
2. **dbt Validation**
   - Validates dbt project structure with dbt debug
   - Lists dbt models and macros (dbt list)
   - No actual database connections are made; mock credentials are used
```

Note: The CI workflow does not run the data pipeline or connect to Postgres, MongoDB, ClickHouse, or MinIO. Its goal is to ensure code quality, dbt project integrity, and basic structure correctness.

### Running Locally
```bash
# Lint Python code
ruff check dagster_code/ --exclude dagster_code/clickhouse_load_tool/
black --check dagster_code/ --exclude dagster_code/clickhouse_load_tool/

# Validate dbt project structure
cd dbt_project/nomba_dbt
dbt debug --profiles-dir .
dbt list --profiles-dir .
```

### Production Considerations
In a production environment, I would add:
- Integration tests with live databases
- Execution of dbt models and data tests
- Automated deployment to staging/production
- Performance benchmarking and monitoring

--- 

## Project Structure
```
nomba-data-pipeline/
├── dagster_code/
│   ├── assets/
│   │   ├── extract_assets.py    # MongoDB & PostgreSQL extraction
│   │   └── dbt_assets.py        # dbt transformation orchestration
│   ├── clickhouse_load_tool/    # Custom ELT framework
│   ├── jobs/                    # transformation jobs
│   │   └── all_jobs.py  
│   ├── schedules/               # transformation job schedules
│   │   └── all_schedules.py  
│   ├── resources/
│   │   └── config.py            # Connection configs
│   │   └── dbt_resources.py  
│   └── definitions.py           # Dagster definitions
│
├── dbt_project/nomba_dbt/
│   ├── snapshots/
│   │   ├── users_snapshot/   
│   ├── models/
│   │   ├── staging/             # Clean & standardize
│   │   │   ├── stg_users.sql
│   │   │   ├── stg_savings_plan.sql
│   │   │   └── stg_savings_transaction.sql
│   │   └── marts/               # Business layer
│   │       ├── dim_users.sql
│   │       ├── dim_savings_plan.sql
│   │       └── fact_savings_transactions.sql
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── setup/
│   ├── generate_data.py         # Sample data generator
│   ├── simulate_cdc.py          # CDC testing script
│   ├── init-mongo.js            # MongoDB initialization
│   └── init-postgres.sql        # PostgreSQL schema
│
├── .github/workflows/
│   └── ci.yml                   # CI/CD pipeline
│
├── docker-compose.yml
├── dagster.yaml
├── workspace.yaml
├── Dockerfile.dagster
├── .env.example
└── README.md.                   # This file
```
___

## Configuration

### Service Ports
- **Dagster UI:** http://localhost:3001
- **PeerDB UI:** http://localhost:9900
- **MinIO Console:** http://localhost:9004
- **ClickHouse:** http://localhost:8124/play
- **PostgreSQL:** localhost:5434
- **MongoDB:** localhost:27017

### Environment Variables
See .env.example for all configuration options

## What I'd Do in Production

1. **Monitoring**: Add Datadog/Prometheus metrics for pipeline health
2. **Alerting**: PagerDuty integration for failures
3. **Partitioning**: Partition large tables by date for query performance
4. **Data contracts**: Great Expectations for input validation
5. **Secrets management**: Vault instead of .env files
6. **Consider log-based CDC**: If latency requirements drop below 5 minutes
7. **Add data catalog**: Data discovery and documentation
8. **Cost optimization**: Compress historical data, archive old partitions


## Key Takeaways

This pipeline demonstrates three critical data engineering skills:

1. **Adapting CDC strategy to source capabilities**: Not every system has perfect audit columns—you work with what you have
2. **Choosing simplicity over complexity**: Timestamp CDC is simpler than log-based and meets requirements
3. **Leveraging existing tools**: dbt snapshots solve SCD Type 2 better than custom code

The result is a **production-ready pipeline** that's maintainable, testable, and appropriate for the scale.

---

## Author

Blessing Angus - [Email](blangus.c@gmail.com)
