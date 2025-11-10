"""
simulate_cdc.py
Simulate realistic CDC (Change Data Capture) activity for testing PeerDB streaming.
Generates inserts and updates for PostgreSQL and MongoDB using existing configs.
"""

import random
import uuid
from datetime import datetime, timedelta
import psycopg2
from pymongo import MongoClient
from psycopg2.extras import execute_batch
from faker import Faker

# Import configs from Dagster project
from dagster_code.resources.config import POSTGRES_CONFIG, MONGO_CONFIG


# Load Profiles
PROFILE = "medium"  # options: "light" | "medium" | "heavy"

LOAD_PROFILES = {
    "light":  {"plans_insert": 200,  "plans_update": 100,  "txns_insert": 2000,  "txns_update": 500,  "users_insert": 50,   "users_update": 100},
    "medium": {"plans_insert": 1000, "plans_update": 500,  "txns_insert": 10000, "txns_update": 2000, "users_insert": 200,  "users_update": 500},
    "heavy":  {"plans_insert": 5000, "plans_update": 2000, "txns_insert": 50000, "txns_update": 10000,"users_insert": 1000, "users_update": 2000}
}

CFG = LOAD_PROFILES[PROFILE]


# Constants
fake = Faker()
PRODUCT_TYPES = ['fixed_savings', 'target_savings', 'flexi_savings']
FREQUENCIES = ['daily', 'weekly', 'monthly']
OCCUPATIONS = ['Engineer', 'Doctor', 'Teacher', 'Trader', 'Entrepreneur']
STATES = ['Lagos', 'Abuja', 'Kano', 'Rivers', 'Oyo', 'Ogun']


# PostgreSQL CDC Simulation
def simulate_postgres_cdc():
    """Simulate inserts and updates for PostgreSQL tables."""
    conn = psycopg2.connect(
        host=POSTGRES_CONFIG["postgres_host"],
        port=POSTGRES_CONFIG["postgres_port"],
        user=POSTGRES_CONFIG["postgres_user"],
        password=POSTGRES_CONFIG["postgres_password"],
        database=POSTGRES_CONFIG["postgres_database"]
    )
    cur = conn.cursor()

    # --- Update existing savings plans ---
    cur.execute("SELECT plan_id FROM savings_plan LIMIT %s", (CFG['plans_update'],))
    for (plan_id,) in cur.fetchall():
        cur.execute("""
            UPDATE savings_plan
            SET amount = amount * %s,
                status = CASE WHEN random() < 0.1 THEN 'completed' ELSE status END,
                updated_at = NOW()
            WHERE plan_id = %s
        """, (random.uniform(1.01, 1.15), plan_id))

    # --- Insert new savings plans ---
    cur.execute("SELECT DISTINCT customer_uid FROM savings_plan LIMIT %s", (CFG['plans_insert'],))
    user_ids = [r[0] for r in cur.fetchall()]
    new_plans = []
    for uid in user_ids:
        start = datetime.now().date()
        end = start + timedelta(days=random.randint(90, 720))
        new_plans.append((
            str(uuid.uuid4()),
            random.choice(PRODUCT_TYPES),
            uid,
            round(random.uniform(5000, 300000), 2),
            random.choice(FREQUENCIES),
            start,
            end,
            'active',
            datetime.now(),
            datetime.now(),
            None
        ))

    execute_batch(cur, """
        INSERT INTO savings_plan (plan_id, product_type, customer_uid, amount, frequency, start_date, end_date, status, created_at, updated_at, deleted_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, new_plans, page_size=1000)

    # --- Insert new transactions ---
    cur.execute("SELECT plan_id FROM savings_plan ORDER BY RANDOM() LIMIT %s", (CFG['txns_insert'],))
    plan_ids = [r[0] for r in cur.fetchall()]
    new_txns = []
    for pid in plan_ids:
        new_txns.append((
            str(uuid.uuid4()),
            pid,
            round(random.uniform(100, 10000), 2),
            'NGN',
            random.choice(['buy', 'sell']),
            round(random.uniform(0.95, 1.05), 4),
            datetime.now(),
            datetime.now(),
            None
        ))

    execute_batch(cur, """
        INSERT INTO savingsTransaction (txn_id, plan_id, amount, currency, side, rate, txn_timestamp, updated_at, deleted_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, new_txns, page_size=2000)

    # --- Update existing transactions ---
    cur.execute("SELECT txn_id FROM savingsTransaction ORDER BY RANDOM() LIMIT %s", (CFG['txns_update'],))
    for (txn_id,) in cur.fetchall():
        cur.execute("""
            UPDATE savingsTransaction
            SET rate = rate * %s,
                updated_at = NOW()
            WHERE txn_id = %s
        """, (random.uniform(0.98, 1.05), txn_id))

    conn.commit()
    cur.close()
    conn.close()


# MongoDB CDC Simulation
def simulate_mongodb_snapshot():
    """Simulate MongoDB user snapshot changes."""
    client = MongoClient(MONGO_CONFIG["mongo_uri"])
    db = client[MONGO_CONFIG["mongo_database"]]
    users = db["users"]

    # Update existing users
    for user in users.find().limit(CFG['users_update']):
        users.update_one(
            {'_id': user['_id']},
            {'$set': {
                'occupation': random.choice(OCCUPATIONS),
                'state': random.choice(STATES)
            }}
        )

    # Insert new users
    count = users.count_documents({})
    new_users = [{
        "_Uid": f"UID{str(count + i + 1).zfill(8)}",
        "firstName": fake.first_name(),
        "lastName": fake.last_name(),
        "occupation": random.choice(OCCUPATIONS),
        "state": random.choice(STATES)
    } for i in range(CFG['users_insert'])]

    if new_users:
        users.insert_many(new_users)

    client.close()

# Main Entry Point
def main():
    print(f"\n🌀 Simulating CDC activity ({PROFILE.upper()} load profile)...")
    start = datetime.now()

    simulate_postgres_cdc()
    simulate_mongodb_snapshot()

    elapsed = (datetime.now() - start).total_seconds()
    print(f"✅ CDC simulation complete in {elapsed:.1f}s\n")

    print(f"Summary ({PROFILE} profile):")
    print(f" - PostgreSQL → {CFG['plans_insert']} new plans, {CFG['txns_insert']} txns inserted")
    print(f" - MongoDB → {CFG['users_insert']} new users")
    print(f" - Updates applied across all datasets\n")

if __name__ == "__main__":
    main()







# """
# simulate_cdc.py
# Simulate Change Data Capture (CDC) activity for testing PeerDB streaming.
# Generates realistic inserts and updates for MongoDB and PostgreSQL.
# """

# import psycopg2
# from pymongo import MongoClient
# from datetime import datetime, timedelta
# import random
# import uuid

# # Configuration
# POSTGRES_CONFIG = {
#     'host': 'localhost',
#     'port': 5434,
#     'database': 'nomba',
#     'user': 'nomba_user',
#     'password': 'nomba_pass'
# }

# MONGO_URI = 'mongodb://admin:password@localhost:27017/'
# MONGO_DB = 'nomba'


# def test_postgres_cdc():
#     """Test CDC for PostgreSQL tables (savings_plan and savingsTransaction)"""
#     print("\n" + "="*60)
#     print("TESTING POSTGRESQL CDC")
#     print("="*60 + "\n")
    
#     conn = psycopg2.connect(**POSTGRES_CONFIG)
#     cursor = conn.cursor()
    
#     # 1. UPDATE existing records
#     print("1. Updating 100 existing savings_plan records...")
#     cursor.execute("""
#         UPDATE savings_plan 
#         SET amount = amount * 1.1,
#             updated_at = NOW()
#         WHERE plan_id IN (
#             SELECT plan_id FROM savings_plan LIMIT 100
#         )
#     """)
#     updated_plans = cursor.rowcount
#     print(f"    Updated {updated_plans} plans")
    
#     # 2. INSERT new records
#     print("\n2. Inserting 50 new savings_plan records...")
#     new_plans = []
#     cursor.execute("SELECT customer_uid FROM savings_plan LIMIT 50")
#     user_ids = [row[0] for row in cursor.fetchall()]
    
#     for uid in user_ids:
#         cursor.execute("""
#             INSERT INTO savings_plan 
#             (product_type, customer_uid, amount, frequency, start_date, end_date, status)
#             VALUES (%s, %s, %s, %s, %s, %s, %s)
#         """, (
#             random.choice(['fixed_savings', 'target_savings', 'flexi_savings']),
#             uid,
#             round(random.uniform(5000, 100000), 2),
#             random.choice(['daily', 'weekly', 'monthly']),
#             datetime.now().date(),
#             (datetime.now() + timedelta(days=180)).date(),
#             'active'
#         ))
#     print(f" Inserted 50 new plans")
    
#     # 3. INSERT new transactions for updated plans
#     print("\n3. Inserting 200 new transactions...")
#     cursor.execute("SELECT plan_id FROM savings_plan LIMIT 100")
#     plan_ids = [row[0] for row in cursor.fetchall()]
    
#     for _ in range(200):
#         cursor.execute("""
#             INSERT INTO savingsTransaction 
#             (plan_id, amount, currency, side, rate, txn_timestamp)
#             VALUES (%s, %s, %s, %s, %s, %s)
#         """, (
#             random.choice(plan_ids),
#             round(random.uniform(100, 10000), 2),
#             'NGN',
#             random.choice(['buy', 'sell']),
#             round(random.uniform(0.95, 1.05), 4),
#             datetime.now()
#         ))
#     print(f"    Inserted 200 new transactions")
    
#     # 4. UPDATE existing transactions
#     print("\n4. Updating 50 existing transactions...")
#     cursor.execute("""
#         UPDATE savingsTransaction 
#         SET rate = rate * 1.02,
#             updated_at = NOW()
#         WHERE txn_id IN (
#             SELECT txn_id FROM savingsTransaction LIMIT 50
#         )
#     """)
#     updated_txns = cursor.rowcount
#     print(f"    Updated {updated_txns} transactions")
    
#     conn.commit()
#     cursor.close()
#     conn.close()
    
#     print("\n PostgreSQL CDC test data created!")
#     print("   - Updated plans: 100")
#     print("   - New plans: 50")
#     print("   - New transactions: 200")
#     print("   - Updated transactions: 50")


# def test_mongodb_snapshot():
#     """Test snapshot CDC for MongoDB users collection"""
#     print("\n" + "="*60)
#     print("TESTING MONGODB SNAPSHOT CDC")
#     print("="*60 + "\n")
    
#     client = MongoClient(MONGO_URI)
#     db = client[MONGO_DB]
#     collection = db['users']
    
#     # 1. UPDATE existing users (change occupation/state)
#     print("1. Updating 100 existing users...")
#     users = list(collection.find().limit(100))
    
#     occupations = ['Engineer', 'Doctor', 'Teacher', 'Trader', 'Entrepreneur']
#     states = ['Lagos', 'Abuja', 'Kano', 'Rivers', 'Oyo']
    
#     for user in users:
#         collection.update_one(
#             {'_id': user['_id']},
#             {'$set': {
#                 'occupation': random.choice(occupations),
#                 'state': random.choice(states)
#             }}
#         )
#     print(f"    Updated 100 users")
    
#     # 2. INSERT new users
#     print("\n2. Inserting 50 new users...")
#     from faker import Faker
#     fake = Faker()
    
#     existing_count = collection.count_documents({})
#     new_users = []
    
#     for i in range(50):
#         new_users.append({
#             "_Uid": f"UID{str(existing_count + i + 1).zfill(8)}",
#             "firstName": fake.first_name(),
#             "lastName": fake.last_name(),
#             "occupation": random.choice(occupations),
#             "state": random.choice(states)
#         })
    
#     collection.insert_many(new_users)
#     print(f"    Inserted 50 new users")
    
#     client.close()
    
#     print("\n MongoDB test data created!")
#     print("   - Updated users: 100")
#     print("   - New users: 50")
#     print("\n IMPORTANT: Now run your Dagster pipeline to capture these changes:")
#     print("   1. MongoDB pipeline will create NEW snapshot with today's date")
#     print("   2. PostgreSQL pipelines will capture new/updated records via updated_at")


# def verify_changes():
#     """Verify the changes were made"""
#     print("\n" + "="*60)
#     print("VERIFYING CHANGES")
#     print("="*60 + "\n")
    
#     # PostgreSQL verification
#     conn = psycopg2.connect(**POSTGRES_CONFIG)
#     cursor = conn.cursor()
    
#     cursor.execute("""
#         SELECT COUNT(*) FROM savings_plan 
#         WHERE updated_at > NOW() - INTERVAL '5 minutes'
#     """)
#     recent_plans = cursor.fetchone()[0]
#     print(f"PostgreSQL plans modified in last 5 min: {recent_plans}")
    
#     cursor.execute("""
#         SELECT COUNT(*) FROM savingsTransaction 
#         WHERE updated_at > NOW() - INTERVAL '5 minutes'
#     """)
#     recent_txns = cursor.fetchone()[0]
#     print(f"PostgreSQL transactions modified in last 5 min: {recent_txns}")
    
#     cursor.close()
#     conn.close()
    
#     # MongoDB verification
#     client = MongoClient(MONGO_URI)
#     db = client[MONGO_DB]
#     total_users = db['users'].count_documents({})
#     print(f"MongoDB total users: {total_users}")
#     client.close()


# if __name__ == "__main__":
#     print("\n CDC TEST SIMULATOR")
#     print("This script will:")
#     print("  1. Update existing records in PostgreSQL")
#     print("  2. Insert new records in PostgreSQL")
#     print("  3. Update existing users in MongoDB")
#     print("  4. Insert new users in MongoDB")
#     print("\nAfter running this, re-run your Dagster + dbt pipeline to test CDC!")
    
#     input("\nPress Enter to continue...")
    
#     try:
#         test_postgres_cdc()
#         test_mongodb_snapshot()
#         verify_changes()
        
#         print("\n" + "="*60)
#         print(" CDC TEST DATA CREATED SUCCESSFULLY!")
#         print("="*60)
#         print("\nNEXT STEPS:")
#         print("1. Run Dagster pipeline to extract changes:")
#         print("   - raw_users (new snapshot)")
#         print("   - raw_plans (incremental)")
#         print("   - raw_savings_transactions (incremental)")
#         print("\n2. Run dbt models:")
#         print("   dbt run --select staging")
#         print("   dbt run --select marts")
#         print("\n3. Verify in ClickHouse:")
#         print("   - Check row counts increased")
#         print("   - Check updated_at timestamps")
#         print("   - Check snapshot_date for users")
        
#     except Exception as e:
#         print(f"\n Error: {str(e)}")
#         raise
