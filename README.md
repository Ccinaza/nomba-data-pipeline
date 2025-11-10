# Nomba Data Pipeline - Technical Assessment

## Executive Summary

This project demonstrates a production-grade CDC (Change Data Capture) pipeline that extracts data from MongoDB and PostgreSQL, loads it into ClickHouse, and transforms it using dbt. The pipeline handles the real-world challenge of capturing changes from a MongoDB collection that lacks timestamp fields.

**Key Features:**
- Handles CDC from sources with and without timestamp fields
- Idempotent execution (run multiple times without duplicates)
- Scalable architecture (processes 150K users, around 3M transactions)
- Automated orchestration with Dagster
- Dimensional modeling with dbt

---

## Problem Statement & Solution Design

### The Challenge

**Requirement:** Build a data pipeline that:
1. Moves data from MongoDB (users) and PostgreSQL (savings_plan, savingsTransaction) to ClickHouse
2. Implements CDC for new and updated records
3. Runs multiple times without creating duplicates
4. **Key constraint:** Some collections lack CDC tracking fields

### Critical Design Decision: How to Handle MongoDB CDC Without Timestamp Field?

**MongoDB users collection:**
```javascript
{
  "_id": ObjectId("..."),
  "_Uid": "UID00000001",
  "firstName": "Ada",
  "lastName": "Obi",
  "occupation": "Engineer",
  "state": "Lagos"
  // No updated_at field
  // No created_at field
}
```

**Options Evaluated:**

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| **Daily Snapshots** | Captures all changes<br> Simple to implement<br> Idempotent |  Full collection scan daily<br> Storage grows over time | **CHOSEN** |
| Hash-based CDC | Detects changes<br> Only loads changed docs | Still requires full scan<br> Complex hash management<br> Can't detect deletes | Rejected |
| MongoDB Change Streams | Real-time<br> Only captures actual changes | Requires replica set<br> Persistent listener<br> Overkill for batch |  Rejected |
| Full Refresh | Simple | Loses history<br> Not true CDC | Rejected |

**Solution: Daily Snapshot CDC With Partitioning + TTL**

**How it works:**
- Extracts entire collection daily
- Tags with snapshot_date = today()
- Delete same-day data before inserting (idempotent)
- Partition by month, 90-day TTL (bounded storage)
- Staging layer deduplicates to latest snapshot per user

**How it achieves requirements:**
1. **Handles new records:** Each day's snapshot includes new users
2. **Handles updates:** New snapshot shows updated state
3. **No duplicates:** Idempotent by date
```python
   # Run 1: Day 1: Extract 150K users → snapshot_date='2024-11-10'
   # Run 2: Day 1 (re-run): Delete snapshot_date='2024-11-10', insert fresh
   # Result: Still 150K rows (not 300K)

   # Run 3 (Tuesday):     Append 150K (assuming no new users) with snapshot_date='2024-11-11'  
   # Result: 300K total (historical tracking)
```
4. **Audit trail:** Historical snapshots enable point-in-time analysis

## Architecture Overview
```
┌─────────────────────────────────────────────────────────┐
│ SOURCE SYSTEMS                                          │
│  ├─ MongoDB: users (no updated_at field)                │
│  └─ Solution: Daily snapshots                           │
│                                                         │
│  ├─ PostgreSQL (savings_plan, savingsTransaction)       │
│  ├─ Has updated_at field                                │
│  └─ Solution: Incremental CDC                           │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ EXTRACTION & STAGING (Dagster Assets + MinIO)           │
│                                                         │
│  Custom ClickHouse Load Tool                            │
│  ├─ MongoDB > MinIO > ClickHouse (Snapshot CDC)         │
│  └─ PostgreSQL > MinIO > ClickHouse (Incremental CDC)   │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ RAW LAYER (ClickHouse)                                  │
│                                                         │
│  raw_users (Snapshot CDC)                               │
│  ├─ Partitioned by month                                │
│  ├─ 90-day TTL retention                                │
│  └─ Append-only (historical audit)                      │
│                                                         │
│  raw_plans (Incremental CDC)                            │
│                                                         │
│  raw_transactions (Incremental CDC)                     │
│  ├─ Uses updated_at tracking                            │
│  └─ Upsert on load                                      │
└─────────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────┐
│ TRANSFORMATION (dbt)                                    │
│  ├─ Staging: Clean & standardize                        │
│  ├─ Dimensions: Current state (dim_users, dim_plans)    │
│  └─ Facts: Transaction grain (fact_transactions)        │
└─────────────────────────────────────────────────────────┘
```
### Key Technical Challenges & Solutions

#### Challenge 1: MongoDB CDC Without Timestamp
**Solution: Daily snapshot CDC (detailed above)**

**Storage Strategy:**
I also encountered a critical issue during implementation: the raw_users table would grow to 13.5M rows (150K × 90 days). Initial attempts without partitioning caused:
- Memory errors during queries
- Slow dbt model execution
- Table scans of millions of rows

**Solution implemented: Storage optimization. How?**
- Partitioned by month: Efficient data pruning
- I set a 90-day TTL: Auto-cleanup (max 13.5M rows)
- I used ORDER BY for partition pruning in queries

**Impact:**
- Query only touches relevant partition (10x faster)
- Old data auto-deleted (no manual cleanup)
- Storage bounded (13.5M rows or 90 days worths of data max)

---

#### Challenge 2: Large Transaction Volume (20M rows)

**Problem:**

Initial data generation created 20M transactions (5.2GB file). First extraction attempt failed:
```
Error: Memory limit exceeded. Would use 3.45 GiB, maximum: 3.44 GiB
```

**Root causes: Root cause: Docker container limits + full table extraction**

**Iterations:**
1. **Increase Docker memory** > Failed (Config issues)
2. **Batch processing in Load Tool** > Helped but insufficient
3. **Manual S3 load** > Worked but not production-grade

4. **Final solution: Data volume adjustment + Partitioning**
    - Reduced to 150K users (~3M transactions, realistic scale)
    - Added monthly partitioning to raw tables
    - Result: Smooth execution

---

## Technical Implementation Details

### 1. CDC Implementation Details

#### PostgreSQL Tables (Has updated_at)
**Strategy: Incremental CDC**
**Process:**
- Track MAX(updated_at) in ClickHouse
- Query: WHERE updated_at > last_loaded_value
- Extract only changed records to MinIO
- Load to ClickHouse with upsert (DELETE matching keys + INSERT)

**dbt incremental:**
```sql
-- example: stg_savings_plan.sql
{{ config(
    materialized='incremental',
    unique_key='plan_id',
    incremental_strategy='delete+insert'  -- Upsert pattern
) }}

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

#### MongoDB Users (No updated_at)
**Strategy: Snapshot CDC (explained above)**

**dbt handling:**
```sql
-- Latest snapshot per user (window function)
with latest_snapshot as (
    select *,
        row_number() over (
            partition by user_id 
            order by snapshot_date desc
        ) as rn
    from {{ ref('stg_users') }}
)
select * from latest_snapshot where rn = 1
```

This ensures:
- One row per user in dimension
- Shows current state (latest snapshot)
- Historical snapshots preserved in staging

#### Testing CDC
**Simulation script: setup/simulate_cdc.py**
```bash
python setup/simulate_cdc.py

# On every run, this file will:
# - Update 100 existing plans
# - Insert 50 new plans
# - Update 50 existing transactions
# - Insert 200 new transactions
# - Update 100 existing users
# - Insert 50 new users
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
Proves: Incremental CDC works, no full reloads, no duplicates

### 2. Custom ClickHouse Load Tool

**Why custom vs. using existing tools?**
- Lightweight (single Python library)
- Full control over CDC logic
- Works locally without cloud dependencies
- Extensible to multiple sources (PostgreSQL, Google Sheets, MongoDB)

**Key features:**
- Generator pattern (constant memory usage)
- Supports snapshot, incremental, full-refresh loads
- Built-in idempotency (upsert patterns)
- Sources: MongoDB, PostgreSQL (extensible to others)

**Design: Object-oriented with abstract base class for common CDC logic, source-specific implementations for extract methods**

---

### 3. Dagster Orchestration

**Why Dagster over Airflow?**

For this assessment:
- Native dbt integration
- Asset-centric (focuses on data, not tasks)
- Lightweight (Docker runs all components)
- Better local development experience

---

### 4. dbt Transformations

**Layered architecture:**
```
staging/                      # Clean and standardize
├─ stg_users.sql              # Column renaming, type casting
├─ stg_savings_plan.sql       # Timezone conversion
└─ stg_savings_transaction.sql

marts/                        # Business logic
├─ dim_users.sql              # Latest snapshot per user (window function)
├─ dim_savings_plan.sql       # Enriched with user data
└─ fact_savings_transactions.sql  # Transaction grain, foreign keys
```
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

##  Quick Start

### Prerequisites

- Docker Desktop
- Python 3.11+
- 16GB RAM minimum

### 1. Clone and Setup
```bash
git clone https://github.com/Ccinaza/nomba-data-pipeline.git
cd nomba-data-pipeline
cp .env.example .env  # Configure if needed
```

### 2. Start Services
```bash
docker-compose up -d

# Wait for health checks (~30 seconds)
docker-compose ps  # All should show "healthy"
```

### 3. Generate Sample Data
```bash
python setup/generate_data.py
```

### 4. Create MinIO Bucket

- Open: http://localhost:9004
- Login: minioadmin / minioadmin
- Create bucket: `nomba-staging`

### 5. Run Pipeline

**Dagster UI:** http://localhost:3001

1. Materialize extraction assets:
   - `raw_users`
   - `raw_plans`
   - `raw_savings_transactions`

2. Materialize dbt models:
   - `All assets` under dbt group

### 6. Verify Data
```bash
docker exec nomba-clickhouse clickhouse-client --query "
SELECT 
    'raw_users' as table, count() as rows FROM nomba.raw_users
UNION ALL
SELECT 'raw_plans', count() FROM nomba.raw_plans
UNION ALL
SELECT 'raw_savings_transactions', count() FROM nomba.raw_savings_transactions
"
```

### Simulate Changes
```bash
python setup/simulate_cdc.py
```

### Verify Incremental Loading
```bash
# Verify in ClickHouse
docker exec nomba-clickhouse clickhouse-client --query "
SELECT 
    max(updated_at) as latest_update,
    count() as total_rows
FROM nomba.raw_plans
"
```

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

## Performance
### Pipeline Duration
- Data Generation: ~20 minutes (150K users, 3M transactions)
- Extraction: 5-10 minutes
- dbt Transformation: 2-5 minutes
- Total: ~15 minutes end-to-end (after data exists)

### Query Performance
- Users: ~10ms (150K rows)
- Plans: ~50ms (225K rows)
- Transactions: ~500ms (3M rows, partitioned)


##  Other Design Decisions

### Why MinIO?

**Problem:** Need staging layer for ELT  
**Alternatives:** Direct load, S3  
**Decision:** Self-hosted MinIO  
**Rationale:**
- No cloud costs
- S3-compatible API
- Works with existing Load Tool
- Reproducible locally

### Why delete+insert for incremental?

**Problem:** ClickHouse doesn't have native UPSERT  
**Alternatives:** ReplacingMergeTree, manual merge  
**Decision:** delete+insert in dbt  
**Rationale:**
- Guarantees consistency
- Simple to reason about
- Performant with unique_key

##  Future Improvements

1. **Real-time CDC:** MongoDB Change Streams for sub-minute latency
2. **Data Quality:** Great Expectations integration
3. **Monitoring:** Grafana dashboards for pipeline health
4. **Cost Optimization:** Compression, tiered storage
5. **Testing:** Automated integration tests with test databases

## Author

Blessing Angus - [Email](blangus.c@gmail.com)
