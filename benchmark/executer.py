"""
This files triggers and executes the benchmark and generates the metrics file
with the results.
"""

import os
import time

import psycopg2
from benchmarker import Benchmarker, Connection

username: str | None = os.getenv("POSTGRES_USER")
password: str | None = os.getenv("POSTGRES_PASSWORD")
host: str = "localhost"
port: int = 5433
database: str = "benchmarker"

# Check for the necessary environment variables before running the benchmark
if username is None or password is None:
    raise ValueError(
        "Environment variables POSTGRES_USER and POSTGRES_PASSWORD must be set."
    )

# Wait for Postgres to be ready for 10 minutes max
for i in range(600):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM tpch.lineitem;")
        row = cur.fetchone()
        if row and row[0] == 59986052:
            conn.close()
            break
        else:
            print(f"Waiting {i + 1}/600 seconds for data to be load...")
            time.sleep(1)
        break
    except psycopg2.Error as e:
        print(f"[{i + 1}/600] Waiting for Postgres/data: {e}")
        time.sleep(1)
else:
    raise TimeoutError("Postgres did not become ready within 10 minutes.")

print("Postgres is ready. Starting benchmark...")
postgres_conn: Connection = Connection(
    db_name=database,
    user=username,
    password=password,
    host=host,
    port=port,
)

benchmarker: Benchmarker = Benchmarker(postgres_conn, export_metrics=True)
benchmarker.measure()
