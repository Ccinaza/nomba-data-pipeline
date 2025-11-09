# setup/generate_data.py
"""
Simple data generator for Nomba assessment
Generates sample data for MongoDB and PostgreSQL
"""

# setup/generate_data.py
import random
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker

fake = Faker()

# SMALLER DATA FOR ASSESSMENT
NUM_USERS = 150_000 
PLANS_PER_USER_RANGE = (0, 2)  # ← Max 2 plans per user
TRANSACTIONS_PER_PLAN_RANGE = (10, 30)  # 10-30 transactions

NIGERIAN_STATES = [
    'Lagos', 'Abuja', 'Kano', 'Rivers', 'Oyo', 'Kaduna',
    'Enugu', 'Delta', 'Ogun', 'Anambra'
]

OCCUPATIONS = [
    'Engineer', 'Doctor', 'Teacher', 'Trader', 'Entrepreneur',
    'Student', 'Civil Servant', 'Banker'
]

PRODUCT_TYPES = [
    'fixed_savings', 'target_savings', 'flexi_savings'
]

def connect_mongodb():
    client = MongoClient('mongodb://admin:password@localhost:27017/')
    return client['nomba']

def connect_postgres():
    return psycopg2.connect(
        host='localhost', port=5434, database='nomba',
        user='nomba_user', password='nomba_pass'
    )

def generate_users(db, count):
    print(f"Generating {count:,} users...")
    collection = db['users']
    batch_size = 10000
    users = []
    
    for i in range(count):
        user = {
            "_Uid": f"UID{str(i+1).zfill(8)}",
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "occupation": random.choice(OCCUPATIONS),
            "state": random.choice(NIGERIAN_STATES)
        }
        users.append(user)
        
        if len(users) >= batch_size:
            collection.insert_many(users)
            print(f"  Inserted {i+1:,} users...")
            users = []
    
    if users:
        collection.insert_many(users)
    
    print(f" {count:,} users generated\n")
    return collection.count_documents({})

def generate_plans(conn, user_uids):
    print(f"Generating savings plans for {len(user_uids):,} users...")
    cursor = conn.cursor()
    plans = []
    plan_ids = []
    
    for uid in user_uids:
        num_plans = random.randint(*PLANS_PER_USER_RANGE)
        
        for _ in range(num_plans):
            plan_id = uuid.uuid4()
            
            # Ensure end_date > start_date
            start_date = fake.date_between(start_date='-2y', end_date='-30d')
            duration_days = random.randint(90, 730)  # 3 months to 2 years
            end_date = start_date + timedelta(days=duration_days)
            
            status = 'active' if random.random() < 0.8 else 'completed'
            
            plan = (
                str(plan_id),  # Keep as UUID
                random.choice(PRODUCT_TYPES),
                uid,
                round(random.uniform(5000, 500000), 2),
                random.choice(['daily', 'weekly', 'monthly']),
                start_date,
                end_date,
                status
            )
            plans.append(plan)
            plan_ids.append((str(plan_id), uid))
    
    execute_batch(cursor, """
        INSERT INTO savings_plan 
        (plan_id, product_type, customer_uid, amount, frequency, start_date, end_date, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, plans, page_size=5000)
    
    conn.commit()
    print(f" {len(plans):,} plans generated\n")
    return plan_ids

def generate_transactions(conn, plan_ids):
    print(f"Generating transactions for {len(plan_ids):,} plans...")
    cursor = conn.cursor()
    transactions = []
    total_count = 0
    
    for i, (plan_id, uid) in enumerate(plan_ids):
        num_txns = random.randint(*TRANSACTIONS_PER_PLAN_RANGE)
        
        for _ in range(num_txns):
            side = 'buy' if random.random() < 0.75 else 'sell'
            txn_date = fake.date_time_between(start_date='-1y', end_date='now')
            
            txn = (
                str(uuid.uuid4()),  # Keep as UUID
                plan_id,
                round(random.uniform(100, 50000), 2),
                'NGN',
                side,
                round(random.uniform(0.95, 1.05), 4),
                txn_date
            )
            transactions.append(txn)
        
        # Insert in batches with progress
        if len(transactions) >= 10000:
            execute_batch(cursor, """
                INSERT INTO savingsTransaction
                (txn_id, plan_id, amount, currency, side, rate, txn_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, transactions, page_size=5000)
            conn.commit()
            
            total_count += len(transactions)
            print(f"  Inserted {total_count:,} transactions ({i+1:,}/{len(plan_ids):,} plans)...")
            transactions = []
    
    # Insert remaining
    if transactions:
        execute_batch(cursor, """
            INSERT INTO savingsTransaction
            (txn_id, plan_id, amount, currency, side, rate, txn_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, transactions, page_size=5000)
        conn.commit()
        total_count += len(transactions)
    
    print(f" {total_count:,} transactions generated\n")
    return total_count

def main():
    print("\n" + "="*60)
    print("NOMBA DATA GENERATOR (OPTIMIZED)")
    print("="*60 + "\n")
    
    start_time = datetime.now()
    
    try:
        mongo_db = connect_mongodb()
        pg_conn = connect_postgres()
        print(" Connected to databases\n")
        
        user_count = generate_users(mongo_db, NUM_USERS)
        
        user_uids = [doc['_Uid'] for doc in mongo_db['users'].find({}, {'_Uid': 1, '_id': 0})]
        print(f" Retrieved {len(user_uids):,} user UIDs\n")
        
        plan_ids = generate_plans(pg_conn, user_uids)
        txn_count = generate_transactions(pg_conn, plan_ids)
        
        pg_conn.close()
        
        duration = (datetime.now() - start_time).total_seconds()
        print("="*60)
        print("DATA GENERATION COMPLETE!")
        print("="*60)
        print(f"  Users:        {user_count:,}")
        print(f"  Plans:        {len(plan_ids):,}")
        print(f"  Transactions: {txn_count:,}")
        print(f"  Duration:     {duration/60:.1f} minutes")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

# import random
# import uuid
# from datetime import datetime, timedelta
# from pymongo import MongoClient
# import psycopg2
# from psycopg2.extras import execute_batch
# from faker import Faker

# # Initialize Faker
# fake = Faker()

# # Configuration
# NUM_USERS = 150_000  # Start with 150k for demo
# PLANS_PER_USER_RANGE = (0, 3)  # Each user has 0-3 plans
# TRANSACTIONS_PER_PLAN_RANGE = (5, 20)  # Each plan has 5-20 transactions

# # Nigerian states
# NIGERIAN_STATES = [
#     'Lagos', 'Abuja', 'Kano', 'Rivers', 'Oyo', 'Kaduna',
#     'Enugu', 'Delta', 'Ogun', 'Anambra', 'Imo', 'Kwara',
#     'Benue', 'Ondo', 'Edo', 'Plateau', 'Abia', 'Osun'
# ]

# # Occupations
# OCCUPATIONS = [
#     'Engineer', 'Doctor', 'Teacher', 'Trader', 'Entrepreneur',
#     'Student', 'Civil Servant', 'Banker', 'Accountant', 'Lawyer',
#     'Nurse', 'Artisan', 'Driver', 'Farmer', 'Business Owner'
# ]

# # Product types
# PRODUCT_TYPES = [
#     'fixed_savings', 'target_savings', 'flexi_savings'
# ]

# def connect_mongodb():
#     """Connect to MongoDB"""
#     client = MongoClient('mongodb://admin:password@localhost:27017/')
#     db = client['nomba']
#     return db

# def connect_postgres():
#     """Connect to PostgreSQL"""
#     conn = psycopg2.connect(
#         host='localhost',
#         port=5434,
#         database='nomba',
#         user='nomba_user',
#         password='nomba_pass'
#     )
#     return conn

# def generate_users(db, count):
#     """Generate users in MongoDB"""
#     print(f"Generating {count:,} users...")
    
#     collection = db['users']
#     batch_size = 10000
#     users = []
    
#     for i in range(count):
#         user = {
#             "_Uid": f"UID{str(i+1).zfill(8)}",  # UID00000001, UID00000002, etc.
#             "firstName": fake.first_name(),
#             "lastName": fake.last_name(),
#             "occupation": random.choice(OCCUPATIONS),
#             "state": random.choice(NIGERIAN_STATES)
#         }
#         users.append(user)
        
#         # Insert in batches
#         if len(users) >= batch_size:
#             collection.insert_many(users)
#             print(f"  Inserted {i+1:,} users...")
#             users = []
    
#     # Insert remaining
#     if users:
#         collection.insert_many(users)
    
#     print(f"{count:,} users generated")
#     return collection.count_documents({})

# def generate_plans(conn, user_uids):
#     """Generate savings plans in PostgreSQL"""
#     print(f"Generating savings plans for {len(user_uids):,} users...")
    
#     cursor = conn.cursor()
#     plans = []
#     plan_ids = []
    
#     for uid in user_uids:
#         num_plans = random.randint(*PLANS_PER_USER_RANGE)
        
#         for _ in range(num_plans):
#             plan_id = uuid.uuid4()
#             start_date = fake.date_between(start_date='-2y', end_date='today')
#             duration_days = random.randint(90, 730)  # 1 month to 2 years
#             end_date = start_date + timedelta(days=duration_days)
            
#             # 80% active, 20% completed
#             status = 'active' if random.random() < 0.8 else 'completed'
            
#             plan = (
#                 str(plan_id),
#                 random.choice(PRODUCT_TYPES),
#                 uid,
#                 round(random.uniform(5000, 500000), 2),
#                 random.choice(['daily', 'weekly', 'monthly']),
#                 start_date,
#                 end_date,
#                 status
#             )
#             plans.append(plan)
#             plan_ids.append((str(plan_id), uid))
    
#     # Bulk insert
#     execute_batch(cursor, """
#         INSERT INTO savings_plan 
#         (plan_id, product_type, customer_uid, amount, frequency, start_date, end_date, status)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#     """, plans, page_size=5000)
    
#     conn.commit()
#     print(f"{len(plans):,} plans generated")
#     return plan_ids

# def generate_transactions(conn, plan_ids):
#     """Generate transactions for savings plans"""
#     print(f"Generating transactions for {len(plan_ids):,} plans...")
    
#     cursor = conn.cursor()
#     transactions = []
#     total_count = 0
    
#     for plan_id, uid in plan_ids:
#         num_txns = random.randint(*TRANSACTIONS_PER_PLAN_RANGE)
        
#         for _ in range(num_txns):
#             # 75% buys, 25% sells
#             side = 'buy' if random.random() < 0.75 else 'sell'
            
#             # Transaction within last year
#             txn_date = fake.date_time_between(start_date='-1y', end_date='now')
            
#             txn = (
#                 str(uuid.uuid4()),
#                 plan_id,
#                 round(random.uniform(100, 50000), 2),
#                 'NGN',
#                 side,
#                 round(random.uniform(0.95, 1.05), 4),  # Exchange rate variation
#                 txn_date
#             )
#             transactions.append(txn)
    
#     # Bulk insert
#     execute_batch(cursor, """
#         INSERT INTO savingsTransaction
#         (txn_id, plan_id, amount, currency, side, rate, txn_timestamp)
#         VALUES (%s, %s, %s, %s, %s, %s, %s)
#     """, transactions, page_size=5000)
    
#     conn.commit()
#     print(f" {len(transactions):,} transactions generated")

# def main():
#     """Main execution"""
#     print("\n" + "="*60)
#     print("NOMBA DATA GENERATOR")
#     print("="*60 + "\n")
    
#     start_time = datetime.now()
    
#     try:
#         # Connect to databases
#         print("Connecting to databases...")
#         mongo_db = connect_mongodb()
#         pg_conn = connect_postgres()
#         print(" Connected to MongoDB and PostgreSQL\n")
        
#         # Generate MongoDB users
#         user_count = generate_users(mongo_db, NUM_USERS)
        
#         # Get all user UIDs
#         print("\nRetrieving user UIDs...")
#         user_uids = [doc['_Uid'] for doc in mongo_db['users'].find({}, {'_Uid': 1, '_id': 0})]
#         print(f" Retrieved {len(user_uids):,} user UIDs\n")
        
#         # Generate PostgreSQL plans
#         plan_ids = generate_plans(pg_conn, user_uids)
        
#         # Generate PostgreSQL transactions
#         generate_transactions(pg_conn, plan_ids)
        
#         # Close connections
#         pg_conn.close()
        
#         # Summary
#         duration = (datetime.now() - start_time).total_seconds()
#         print("\n" + "="*60)
#         print("DATA GENERATION COMPLETE!")
#         print("="*60)
#         print(f"  Users:        {user_count:,}")
#         print(f"  Plans:        {len(plan_ids):,}")
#         print(f"  Transactions: {len(plan_ids) * 25:,} (estimated)")
#         print(f"  Duration:     {duration:.2f} seconds")
#         print("="*60 + "\n")
        
#     except Exception as e:
#         print(f"\n Error: {str(e)}")
#         raise

# if __name__ == "__main__":
#     main()