"""
This files triggers and executes the benchmark and generates the metrics file
with the results.
"""

import os

from benchmarker import Benchmarker, Connection

username: str | None = os.getenv("POSTGRES_USER")
password: str | None = os.getenv("POSTGRES_PASSWORD")

# Check for the necessary environment variables before running the benchmark
if username is None or password is None:
    raise ValueError(
        "Environment variables POSTGRES_USER and POSTGRES_PASSWORD must be set."
    )

postgres_conn: Connection = Connection(
    db_name="benchmarker",
    user=username,
    password=password,
    port=5433,
)

benchmarker: Benchmarker = Benchmarker(postgres_conn, export_metrics=True)
benchmarker.measure()
