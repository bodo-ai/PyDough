"""
File that tests that the TPCH data is downloaded correctly by
running a simple TPCH query on SQLite.
"""

import sqlite3
import typing as pt

import pytest


def test_tpch_q6(tpch_db: sqlite3.Connection):
    """
    Run the TPCH Q6 query on the SQLite database.
    """
    cur: sqlite3.Cursor = tpch_db.cursor()
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
    result: list[pt.Any] = cur.fetchall()
    assert len(result) == 1, "Expected one row"
    assert len(result[0]) == 1, "Expected one column"
    assert result[0][0] == pytest.approx(123141078.2283, rel=1e-4), "Unexpected result"
