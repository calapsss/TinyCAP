import psycopg2
import pandas as pd
import sys

def create_database_if_not_exists(host, user, password, db_name, port=5432):
    conn = psycopg2.connect(host=host, user=user, password=password, port=port, dbname="postgres")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
    exists = cur.fetchone()
    if not exists:
        print(f"Creating database {db_name}...")
        cur.execute(f"CREATE DATABASE {db_name}")
    else:
        print(f"Database {db_name} already exists.")
    cur.close()
    conn.close()

def create_table_from_csv(conn, table_name, csv_file):
    df = pd.read_csv(csv_file)

    # Infer schema: map pandas dtypes to PostgreSQL
    dtype_map = {
        "object": "TEXT",
        "int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "datetime64[ns]": "TIMESTAMP"
    }

    columns = []
    for col, dtype in df.dtypes.items():
        pg_type = dtype_map.get(str(dtype), "TEXT")
        columns.append(f"\"{col}\" {pg_type}")
    columns_sql = ", ".join(columns)

    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    cur.execute(f"CREATE TABLE {table_name} ({columns_sql})")
    conn.commit()
    cur.close()
    print(f"Created table {table_name} with schema from CSV.")

    return df

def import_csv_to_postgres(host, user, password, db_name, table_name, csv_file, port=5432):
    # Ensure database exists
    create_database_if_not_exists(host, user, password, db_name, port)

    # Connect to target DB
    conn = psycopg2.connect(host=host, user=user, password=password, dbname=db_name, port=port)

    # Create table schema
    df = create_table_from_csv(conn, table_name, csv_file)

    # Insert rows
    cur = conn.cursor()
    for _, row in df.iterrows():
        cols = ", ".join([f"\"{c}\"" for c in df.columns])
        vals = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
        cur.execute(sql, tuple(row.fillna("").tolist()))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Imported {len(df)} rows into {table_name}.")

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Usage: python import_csv_to_postgres.py <host> <user> <password> <db_name> <table_name> <csv_file>")
        sys.exit(1)

    host, user, password, db_name, table_name, csv_file = sys.argv[1:7]
    import_csv_to_postgres(host, user, password, db_name, table_name, csv_file)
