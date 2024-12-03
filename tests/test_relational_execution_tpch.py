"""
Simple tests to run TPC-H queries on SQLite.
"""

import pandas as pd
import pytest
from tpch_outputs import tpch_q1_output, tpch_q3_output, tpch_q6_output
from tpch_relational_plans import (
    tpch_query_1_plan,
    tpch_query_3_plan,
    tpch_query_6_plan,
)

from pydough.database_connectors import DatabaseContext
from pydough.relational import RelationalRoot
from pydough.sqlglot import execute


@pytest.mark.parametrize(
    "root, output",
    [
        pytest.param(
            tpch_query_1_plan(),
            tpch_q1_output(),
            id="tpch_q1",
        ),
        pytest.param(
            tpch_query_3_plan(),
            tpch_q3_output(),
            id="tpch_q3",
        ),
        pytest.param(
            tpch_query_6_plan(),
            tpch_q6_output(),
            id="tpch_q6",
        ),
    ],
)
def test_tpch(
    root: RelationalRoot, output: pd.DataFrame, sqlite_tpch_db_context: DatabaseContext
) -> None:
    """
    Test the example TPC-H relational trees executed on a
    SQLite database.
    """
    result = execute(root, sqlite_tpch_db_context)
    pd.testing.assert_frame_equal(result, output)
