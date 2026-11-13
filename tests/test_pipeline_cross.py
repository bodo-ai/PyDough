"""SQL generation and execution tests for CROSS window scope on SQLite TPC-H."""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.unqualified import UnqualifiedNode
from tests.test_pydough_functions import cross_test_functions as queries

from .testing_utilities import (
    PyDoughPandasTest,
    PyDoughSQLComparisonTest,
    graph_fetcher,
)

# These reference queries describe the expected results independently of the
# compiler. The full TPC-H fixture has different rows from the small repro.
ORDERS = """
WITH selected AS (
    SELECT o_custkey AS c, o_orderkey AS oid, o_totalprice AS amount
    FROM tpch.orders WHERE o_custkey <= 2
), lhs AS (
    SELECT c_custkey AS k FROM tpch.customer WHERE c_custkey <= 2
)
"""
REGIONS = "(SELECT r_regionkey AS rk FROM tpch.region WHERE r_regionkey <= 1)"
NATIONS = "(SELECT n_nationkey AS nk FROM tpch.nation WHERE n_nationkey <= 1)"
RUNNING = """
SUM(amount) OVER (
    PARTITION BY c ORDER BY oid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
"""
PREV = "LAG(oid) OVER (PARTITION BY c ORDER BY oid)"
NEXT = "LEAD(oid) OVER (PARTITION BY c ORDER BY oid)"
ORDERS_TOTAL = "(SELECT SUM(amount) FROM selected)"
RUNNING_RESULT = ORDERS + f"SELECT c, oid, {RUNNING} AS running FROM selected"
FILTERED_RUNNING = RUNNING_RESULT + " WHERE amount > 100000"
NEIGHBORS = (
    ORDERS + f"SELECT c, oid, {PREV} AS prev_oid, {NEXT} AS next_oid FROM selected"
)
MULTI = (
    ORDERS + f"SELECT c, oid, {RUNNING} AS running, {PREV} AS prev_oid FROM selected"
)


def cross_case(query: Callable[[], UnqualifiedNode], reference: str):
    """Pair a PyDough function with independently written SQLite SQL."""
    name = "cross_tpch_" + query.__name__.removeprefix("cross_")
    return pytest.param(
        PyDoughSQLComparisonTest(
            query, "TPCH", lambda: reference, name, fix_column_names=False
        ),
        id=name,
    )


@pytest.fixture(
    params=[
        # Original cases, including standalone and reversed-CROSS controls.
        cross_case(queries.cross_top, RUNNING_RESULT),
        cross_case(queries.cross_crossed, RUNNING_RESULT),
        cross_case(queries.cross_reversed_crossed, RUNNING_RESULT),
        cross_case(queries.cross_top2, RUNNING_RESULT),
        cross_case(queries.cross_crossed2, RUNNING_RESULT),
        cross_case(queries.cross_top_neighbors, NEIGHBORS),
        cross_case(queries.cross_crossed_neighbors, NEIGHBORS),
        cross_case(
            queries.cross_unfiltered_neighbors,
            f"""
            WITH neighbor_rows AS ({NEIGHBORS})
            SELECT c, oid, prev_oid, next_oid FROM neighbor_rows
            CROSS JOIN (SELECT c_custkey FROM tpch.customer WHERE c_custkey <= 2)
        """,
        ),
        cross_case(
            queries.cross_post_cross_window,
            ORDERS
            + """
            SELECT c, oid, SUM(amount) OVER () AS cross_total
            FROM lhs CROSS JOIN selected
        """,
        ),
        cross_case(queries.cross_filtered_top, FILTERED_RUNNING),
        cross_case(queries.cross_filtered_crossed, FILTERED_RUNNING),
        cross_case(
            queries.cross_mixed_windows,
            ORDERS
            + f"""
            , rhs AS (SELECT c, oid, amount, {RUNNING} AS running FROM selected)
            SELECT c, oid, running, SUM(amount) OVER () AS cross_total
            FROM lhs CROSS JOIN rhs
        """,
        ),
        cross_case(queries.cross_top_multi, MULTI),
        cross_case(queries.cross_crossed_multi, MULTI),
        cross_case(
            queries.cross_nested_partition_windows,
            ORDERS
            + f"""
            , rhs AS (SELECT c, oid, amount, {RUNNING} AS running FROM selected),
            inner_rows AS (
                SELECT k, c, oid, amount, running, SUM(amount) OVER () AS inner_total
                FROM lhs CROSS JOIN rhs
            )
            SELECT rk AS r, k, c, oid, running, inner_total,
                   SUM(amount) OVER () AS outer_total
            FROM {REGIONS} CROSS JOIN inner_rows
        """,
        ),
        cross_case(
            queries.cross_without_windows,
            ORDERS
            + """
            SELECT c, oid, amount FROM lhs CROSS JOIN selected
        """,
        ),
        cross_case(
            queries.cross_correlated_partition_source,
            ORDERS
            + """
            SELECT k, c, oid,
                   SUM(amount) OVER (
                       PARTITION BY k, c ORDER BY oid
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS running
            FROM lhs CROSS JOIN selected WHERE amount > k * 100000
        """,
        ),
        # Six cases from the review.
        cross_case(
            queries.cross_rhs_window_no_per,
            ORDERS
            + f"""
            , rhs AS (
                SELECT oid, ROW_NUMBER() OVER (ORDER BY oid) AS r, {ORDERS_TOTAL} AS tot
                FROM selected
            )
            SELECT k, oid, r, tot FROM lhs CROSS JOIN rhs
        """,
        ),
        cross_case(
            queries.cross_rhs_window_per_nation,
            """
            WITH rhs AS (
                SELECT o_orderkey AS oid,
                       SUM(o_totalprice) OVER (PARTITION BY n_nationkey) AS s
                FROM tpch.orders
                JOIN tpch.customer ON o_custkey = c_custkey
                JOIN tpch.nation ON c_nationkey = n_nationkey
                WHERE o_custkey <= 14
            )
            SELECT c_custkey AS k, oid, s
            FROM tpch.customer CROSS JOIN rhs WHERE c_custkey <= 2
        """,
        ),
        cross_case(
            queries.cross_rhs_window_per_customer,
            ORDERS
            + f"""
            , rhs AS (SELECT oid, SUM(amount) OVER (PARTITION BY c) AS s FROM selected)
            SELECT nk, oid, s FROM {NATIONS} CROSS JOIN rhs
        """,
        ),
        cross_case(
            queries.cross_rhs_partition_aggregate,
            ORDERS
            + f"""
            SELECT c, COUNT(*) OVER (PARTITION BY c) AS n, oid, {RUNNING} AS running
            FROM selected
        """,
        ),
        cross_case(queries.cross_rhs_lhs_filter_after_window, RUNNING_RESULT),
        cross_case(queries.cross_rhs_lhs_filter_before_window, RUNNING_RESULT),
        # Additional coverage from the follow-up.
        cross_case(
            queries.cross_rhs_filter_before_global_window,
            ORDERS
            + """
            SELECT c AS k, oid, SUM(amount) OVER (PARTITION BY c) AS total
            FROM selected
        """,
        ),
        cross_case(
            queries.cross_rhs_filter_after_global_window,
            ORDERS
            + f"""
            SELECT c AS k, oid, {ORDERS_TOTAL} AS total FROM selected
        """,
        ),
        cross_case(
            queries.cross_rhs_top_k_before_window,
            ORDERS
            + """
            , first_orders AS (SELECT * FROM selected ORDER BY oid LIMIT 2),
            rhs AS (SELECT oid, SUM(amount) OVER () AS tot FROM first_orders)
            SELECT k, oid, tot FROM lhs CROSS JOIN rhs
        """,
        ),
        cross_case(
            queries.cross_rhs_top_k_after_window,
            ORDERS
            + f"""
            , rhs AS (SELECT oid, {ORDERS_TOTAL} AS tot FROM selected ORDER BY oid LIMIT 2)
            SELECT k, oid, tot FROM lhs CROSS JOIN rhs
        """,
        ),
        cross_case(
            queries.cross_nested_global_windows,
            ORDERS
            + f"""
            , inner_rows AS (
                SELECT k, oid, amount, {ORDERS_TOTAL} AS t,
                       SUM(amount) OVER () AS inner_total
                FROM lhs CROSS JOIN selected
            )
            SELECT rk, k, oid, t, inner_total, SUM(amount) OVER () AS outer_total
            FROM {REGIONS} CROSS JOIN inner_rows
        """,
        ),
        cross_case(
            queries.cross_lhs_has_multiple_levels,
            ORDERS
            + f"""
            SELECT rk, k, oid, {ORDERS_TOTAL} AS t
            FROM {REGIONS} CROSS JOIN lhs CROSS JOIN selected
        """,
        ),
        cross_case(
            queries.cross_window_inside_aggregate,
            ORDERS
            + f"""
            , rhs AS (SELECT oid, {ORDERS_TOTAL} AS t FROM selected)
            SELECT k, (SELECT SUM(t) FROM {REGIONS} CROSS JOIN rhs) AS total FROM lhs
        """,
        ),
        cross_case(
            queries.cross_window_in_rhs_child_subtree,
            ORDERS
            + f"""
            , rhs AS (
                SELECT k AS ck, (
                    SELECT COUNT(*) * SUM(amount) FROM selected WHERE c = k
                ) AS total
                FROM lhs
            )
            SELECT nk, ck, total FROM {NATIONS} CROSS JOIN rhs
        """,
        ),
        cross_case(
            queries.cross_explicit_per_after_join,
            ORDERS
            + """
            SELECT k, c, oid, SUM(amount) OVER (PARTITION BY c) AS total
            FROM lhs CROSS JOIN selected
        """,
        ),
    ],
)
def cross_pipeline_test_data(
    request: pytest.FixtureRequest,
) -> PyDoughSQLComparisonTest:
    return request.param


def test_pipeline_until_sql_tpch(
    cross_pipeline_test_data: PyDoughSQLComparisonTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """Compare generated SQLite SQL with the checked-in snapshot."""
    case = cross_pipeline_test_data
    # The SQL snapshot helper lives on PyDoughPandasTest. It does not execute
    # the expected-frame callback.
    snapshot_case = PyDoughPandasTest(
        case.pydough_function, case.graph_name, pd.DataFrame, case.test_name
    )
    file_path = get_sql_test_filename(case.test_name, sqlite_tpch_db_context.dialect)
    snapshot_case.run_sql_test(
        get_sample_graph, file_path, update_tests, sqlite_tpch_db_context
    )


@pytest.mark.execute
def test_pipeline_e2e_cross(
    cross_pipeline_test_data: PyDoughSQLComparisonTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
) -> None:
    """Compare PyDough results against reference SQL on the same TPC-H fixture."""
    cross_pipeline_test_data.run_e2e_test(
        get_sample_graph, sqlite_tpch_db_context, rtol=1e-9, atol=0.01
    )
