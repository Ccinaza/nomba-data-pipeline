-- Create databases
create database if not exists nomba;
create database if not exists nomba_dev;
create database if not exists nomba_dev_gamma_staging;
create database if not exists nomba_dev_gamma_snapshots;
create database if not exists nomba_dev_gamma_marts;

create user gamma identified with sha256_password by 'aer2DCCpt1nf';

grant create table, create database, insert, update, alter, drop, select, truncate
        on *.* to gamma;

-- -- Raw Tables
create table if not exists nomba.raw_savings_plans (
    plan_id String,
    product_type String,
    customer_uid String,
    amount Float64,
    frequency String,
    start_date Date,
    end_date Nullable(Date),
    status String,
    created_at DateTime,
    updated_at DateTime,
    deleted_at Nullable(DateTime)
) ENGINE = MergeTree()
order by (plan_id, updated_at);

create table if not exists nomba.raw_savings_transactions (
    txn_id String,
    plan_id String,
    amount Float64,
    currency String,
    side String,
    rate Float64,
    txn_timestamp DateTime,
    updated_at DateTime,
    deleted_at Nullable(DateTime)
) ENGINE = MergeTree()
partition by toStartOfMonth(txn_timestamp) 
order by (txn_id, updated_at, txn_timestamp);

-- Raw table for MongoDB users (full load)
create table if not exists nomba.raw_users (
    _id String,
    _Uid  String,
    firstName String,
    lastName String,
    occupation String,
    state String
) ENGINE = MergeTree()
order by (_Uid );
