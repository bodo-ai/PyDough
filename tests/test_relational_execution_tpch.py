"""
Simple tests to run TPC-H queries on SQLite.
"""

from typing import Any

import pytest
from tpch_outputs import tpch_q6_output
from tpch_relational_plans import tpch_query_6_plan

from pydough.database_connectors import DatabaseContext
from pydough.relational import RelationalRoot
from pydough.sqlglot import execute


@pytest.mark.parametrize(
    "root, output",
    [
        pytest.param(
            tpch_query_6_plan(),
            tpch_q6_output(),
            id="tpch_q6",
        ),
    ],
)
def test_tpch(
    root: RelationalRoot, output: list[Any], sqlite_tpch_db_context: DatabaseContext
):
    """
    Test the example TPC-H relational trees executed on a
    SQLite database.
    """
    result = execute(root, sqlite_tpch_db_context)
    assert [pytest.approx(x, abs=0.001) for x in result] == output
