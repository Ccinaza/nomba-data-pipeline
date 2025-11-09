-- setup/init-postgres.sql (FIXED VERSION)

-- Switch to postgres database first, then drop/create nomba
\c postgres;

DROP DATABASE IF EXISTS nomba;
CREATE DATABASE nomba;

\c nomba;

-- Create tables
CREATE TABLE IF NOT EXISTS savings_plan (
    plan_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_type TEXT NOT NULL,
    customer_uid TEXT NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    frequency TEXT NOT NULL CHECK (frequency IN ('daily', 'weekly', 'monthly')),
    start_date DATE NOT NULL,
    end_date DATE,
    status TEXT NOT NULL CHECK (status IN ('active', 'completed', 'cancelled')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS savingsTransaction (
    txn_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plan_id UUID NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    currency TEXT NOT NULL DEFAULT 'NGN',
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    rate NUMERIC(10, 4) NOT NULL CHECK (rate > 0),
    txn_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    FOREIGN KEY (plan_id) REFERENCES savings_plan(plan_id)
);

-- Function to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers (FIXED TABLE NAME)
CREATE TRIGGER update_plan_updated_at 
    BEFORE UPDATE ON savings_plan
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_txn_updated_at 
    BEFORE UPDATE ON savingsTransaction  -- FIXED: was savingsTransactionTrack
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_plan_customer_uid ON savings_plan(customer_uid);
CREATE INDEX IF NOT EXISTS idx_plan_status ON savings_plan(status);
CREATE INDEX IF NOT EXISTS idx_plan_updated_at ON savings_plan(updated_at);
CREATE INDEX IF NOT EXISTS idx_plan_created_at ON savings_plan(created_at);

CREATE INDEX IF NOT EXISTS idx_txn_plan_id ON savingsTransaction(plan_id);
CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON savingsTransaction(txn_timestamp);
CREATE INDEX IF NOT EXISTS idx_txn_updated_at ON savingsTransaction(updated_at);
CREATE INDEX IF NOT EXISTS idx_txn_side ON savingsTransaction(side);

-- Confirmation message
DO $$
BEGIN
    RAISE NOTICE '✓ PostgreSQL initialized successfully';
    RAISE NOTICE '  - Database: nomba';
    RAISE NOTICE '  - Tables: savings_plan, savingsTransaction';
    RAISE NOTICE '  - Triggers and indexes created';
END $$;