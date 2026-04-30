"""
This files triggers and executes the benchmark and generates the metrics file
with the results.
"""

import os

from .benchmarker import Benchmarker, Connection

postgres_conn: Connection = Connection(
    db_name="benchmarker",
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=5433,
)

benchmarker: Benchmarker = Benchmarker(postgres_conn, export_metrics=True)
benchmarker.measure()
