"""
Test CDC by simulating new records and updates
Run this after initial load to test incremental behavior
"""

import psycopg2
from pymongo import MongoClient
from datetime import datetime, timedelta
import random
import uuid

# Configuration
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5434,
    'database': 'nomba',
    'user': 'nomba_user',
    'password': 'nomba_pass'
}

MONGO_URI = 'mongodb://admin:password@localhost:27017/'
MONGO_DB = 'nomba'


def test_postgres_cdc():
    """Test CDC for PostgreSQL tables (savings_plan and savingsTransaction)"""
    print("\n" + "="*60)
    print("TESTING POSTGRESQL CDC")
    print("="*60 + "\n")
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # 1. UPDATE existing records
    print("1. Updating 100 existing savings_plan records...")
    cursor.execute("""
        UPDATE savings_plan 
        SET amount = amount * 1.1,
            updated_at = NOW()
        WHERE plan_id IN (
            SELECT plan_id FROM savings_plan LIMIT 100
        )
    """)
    updated_plans = cursor.rowcount
    print(f"    Updated {updated_plans} plans")
    
    # 2. INSERT new records
    print("\n2. Inserting 50 new savings_plan records...")
    new_plans = []
    cursor.execute("SELECT customer_uid FROM savings_plan LIMIT 50")
    user_ids = [row[0] for row in cursor.fetchall()]
    
    for uid in user_ids:
        cursor.execute("""
            INSERT INTO savings_plan 
            (product_type, customer_uid, amount, frequency, start_date, end_date, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            random.choice(['fixed_savings', 'target_savings', 'flexi_savings']),
            uid,
            round(random.uniform(5000, 100000), 2),
            random.choice(['daily', 'weekly', 'monthly']),
            datetime.now().date(),
            (datetime.now() + timedelta(days=180)).date(),
            'active'
        ))
    print(f" Inserted 50 new plans")
    
    # 3. INSERT new transactions for updated plans
    print("\n3. Inserting 200 new transactions...")
    cursor.execute("SELECT plan_id FROM savings_plan LIMIT 100")
    plan_ids = [row[0] for row in cursor.fetchall()]
    
    for _ in range(200):
        cursor.execute("""
            INSERT INTO savingsTransaction 
            (plan_id, amount, currency, side, rate, txn_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            random.choice(plan_ids),
            round(random.uniform(100, 10000), 2),
            'NGN',
            random.choice(['buy', 'sell']),
            round(random.uniform(0.95, 1.05), 4),
            datetime.now()
        ))
    print(f"    Inserted 200 new transactions")
    
    # 4. UPDATE existing transactions
    print("\n4. Updating 50 existing transactions...")
    cursor.execute("""
        UPDATE savingsTransaction 
        SET rate = rate * 1.02,
            updated_at = NOW()
        WHERE txn_id IN (
            SELECT txn_id FROM savingsTransaction LIMIT 50
        )
    """)
    updated_txns = cursor.rowcount
    print(f"    Updated {updated_txns} transactions")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n PostgreSQL CDC test data created!")
    print("   - Updated plans: 100")
    print("   - New plans: 50")
    print("   - New transactions: 200")
    print("   - Updated transactions: 50")


def test_mongodb_snapshot():
    """Test snapshot CDC for MongoDB users collection"""
    print("\n" + "="*60)
    print("TESTING MONGODB SNAPSHOT CDC")
    print("="*60 + "\n")
    
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db['users']
    
    # 1. UPDATE existing users (change occupation/state)
    print("1. Updating 100 existing users...")
    users = list(collection.find().limit(100))
    
    occupations = ['Engineer', 'Doctor', 'Teacher', 'Trader', 'Entrepreneur']
    states = ['Lagos', 'Abuja', 'Kano', 'Rivers', 'Oyo']
    
    for user in users:
        collection.update_one(
            {'_id': user['_id']},
            {'$set': {
                'occupation': random.choice(occupations),
                'state': random.choice(states)
            }}
        )
    print(f"    Updated 100 users")
    
    # 2. INSERT new users
    print("\n2. Inserting 50 new users...")
    from faker import Faker
    fake = Faker()
    
    existing_count = collection.count_documents({})
    new_users = []
    
    for i in range(50):
        new_users.append({
            "_Uid": f"UID{str(existing_count + i + 1).zfill(8)}",
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "occupation": random.choice(occupations),
            "state": random.choice(states)
        })
    
    collection.insert_many(new_users)
    print(f"    Inserted 50 new users")
    
    client.close()
    
    print("\n MongoDB test data created!")
    print("   - Updated users: 100")
    print("   - New users: 50")
    print("\n IMPORTANT: Now run your Dagster pipeline to capture these changes:")
    print("   1. MongoDB pipeline will create NEW snapshot with today's date")
    print("   2. PostgreSQL pipelines will capture new/updated records via updated_at")


def verify_changes():
    """Verify the changes were made"""
    print("\n" + "="*60)
    print("VERIFYING CHANGES")
    print("="*60 + "\n")
    
    # PostgreSQL verification
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) FROM savings_plan 
        WHERE updated_at > NOW() - INTERVAL '5 minutes'
    """)
    recent_plans = cursor.fetchone()[0]
    print(f"PostgreSQL plans modified in last 5 min: {recent_plans}")
    
    cursor.execute("""
        SELECT COUNT(*) FROM savingsTransaction 
        WHERE updated_at > NOW() - INTERVAL '5 minutes'
    """)
    recent_txns = cursor.fetchone()[0]
    print(f"PostgreSQL transactions modified in last 5 min: {recent_txns}")
    
    cursor.close()
    conn.close()
    
    # MongoDB verification
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    total_users = db['users'].count_documents({})
    print(f"MongoDB total users: {total_users}")
    client.close()


if __name__ == "__main__":
    print("\n CDC TEST SIMULATOR")
    print("This script will:")
    print("  1. Update existing records in PostgreSQL")
    print("  2. Insert new records in PostgreSQL")
    print("  3. Update existing users in MongoDB")
    print("  4. Insert new users in MongoDB")
    print("\nAfter running this, re-run your Dagster + dbt pipeline to test CDC!")
    
    input("\nPress Enter to continue...")
    
    try:
        test_postgres_cdc()
        test_mongodb_snapshot()
        verify_changes()
        
        print("\n" + "="*60)
        print(" CDC TEST DATA CREATED SUCCESSFULLY!")
        print("="*60)
        print("\nNEXT STEPS:")
        print("1. Run Dagster pipeline to extract changes:")
        print("   - raw_users (new snapshot)")
        print("   - raw_plans (incremental)")
        print("   - raw_savings_transactions (incremental)")
        print("\n2. Run dbt models:")
        print("   dbt run --select staging")
        print("   dbt run --select marts")
        print("\n3. Verify in ClickHouse:")
        print("   - Check row counts increased")
        print("   - Check updated_at timestamps")
        print("   - Check snapshot_date for users")
        
    except Exception as e:
        print(f"\n Error: {str(e)}")
        raise
