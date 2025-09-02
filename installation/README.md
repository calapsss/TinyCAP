# CSV to PostgreSQL Importer

This module provides a ready-to-use script to create a PostgreSQL table and import CSV data into it. It is designed to work with structured personnel data (e.g., HR, military, or enterprise datasets) but can be extended for other CSV files.

## Prerequisites
1. Installed requirements.txt
2. Your PostgresDB is Live

## Steps
1. Run the Importer
```
python import_csv_to_postgres.py <host> <user> <password> <db_name> <table_name> <csv_file>
```
2. What the Script Does
- Connects to PostgreSQL with your provided credentials.
- Creates the target database if it does not exist.
- Drops the target table if it exists, then recreates it using the CSV header and inferred types.
- Inserts all rows from the CSV file into the table.

3. Verify Imports
```
psql -h localhost -U <user> -d <db_name>
\dt
SELECT * FROM <table_name> LIMIT 5;