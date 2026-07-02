-- Runs ONLY on first initialization of an empty Postgres data directory.
-- POSTGRES_DB=transaction_db is created automatically by the image; create the second DB here.
CREATE DATABASE fraud_db;
