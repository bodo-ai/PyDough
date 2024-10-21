"""
File that tests that the TPCH data is downloaded correctly by
running a simple TPCH query on SQLite.
"""

import os
import sqlite3

import pytest


@pytest.fixture(scope="module")
def tpch_db():
    """
    Download the TPCH data and return a connection to the SQLite database.
    """
    path = os.environ["TPCH_DB_PATH"]
    conn = sqlite3.connect(path)
    yield conn
    conn.close()


def test_tpch_q6(tpch_db):
    """
    Run the TPCH Q6 query on the SQLite database.
    """
    cur = tpch_db.cursor()
    cur.execute("""
        select
            sum(l_extendedprice * l_discount) as revenue
        from
            lineitem
        where
            l_shipdate >= '1994-01-01'
            and l_shipdate < '1995-01-01'
            and l_discount between 0.05 and 0.07
            and l_quantity < 24
    """)
    result = cur.fetchone()
    assert result[0] == pytest.approx(123141078.2283, rel=1e-4)
