"""
Simple tests to run TPC-H queries on SQLite.
"""

import pandas as pd
import pytest

from pydough.configs import PyDoughSession
from pydough.relational import RelationalRoot
from pydough.sqlglot import execute_df
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q1_output,
    tpch_q3_output,
    tpch_q6_output,
)
from tests.test_pydough_functions.tpch_relational_plans import (
    tpch_query_1_plan,
    tpch_query_3_plan,
    tpch_query_6_plan,
)

pytestmark = [pytest.mark.execute]


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
    root: RelationalRoot,
    output: pd.DataFrame,
    sqlite_tpch_session: PyDoughSession,
) -> None:
    """
    Test the example TPC-H relational trees executed on a
    SQLite database.
    """
    result = execute_df(root, sqlite_tpch_session)
    pd.testing.assert_frame_equal(result, output)
