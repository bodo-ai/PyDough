"""SQL generation and execution tests for CROSS window scope on SQLite TPC-H."""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough import to_df
from pydough.configs import PyDoughSession
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.unqualified import UnqualifiedNode
from tests.test_pydough_functions import cross_test_functions as queries

from .testing_utilities import (
    PyDoughPandasTest,
    graph_fetcher,
    transform_and_exec_pydough,
)

# Expected values for the repository's full TPC-H fixture, not the small repro.
# Each row is (customer, order, amount, running total, previous order, next order).
ORDERS = [
    (1, 454791, 74602.81, 74602.81, None, 579908),
    (1, 579908, 54048.26, 128651.07, 454791, 3868359),
    (1, 3868359, 123076.84, 251727.91, 579908, 4273923),
    (1, 4273923, 95911.01, 347638.92, 3868359, 4808192),
    (1, 4808192, 65478.05, 413116.97, 4273923, 5133509),
    (1, 5133509, 174645.94, 587762.91, 4808192, None),
    (2, 430243, 33082.83, 33082.83, None, 1071617),
    (2, 1071617, 221397.35, 254480.18, 430243, 1374019),
    (2, 1374019, 167016.61, 421496.79, 1071617, 1763205),
    (2, 1763205, 16495.33, 437992.12, 1374019, 1842406),
    (2, 1842406, 174291.41, 612283.53, 1763205, 2992930),
    (2, 2992930, 103297.68, 715581.21, 1842406, 3986496),
    (2, 3986496, 312692.22, 1028273.43, 2992930, None),
]
ORDER_IDS = sorted(oid for _, oid, *_ in ORDERS)
CUSTOMER_KEYS = (1, 2)
REGION_KEYS = (0, 1)
NATION_KEYS = (0, 1)
CUSTOMER_TOTALS = {1: 587762.91, 2: 1028273.43}
RHS_TOTAL = 1616036.34
CROSS_TOTAL = 3232072.68
OUTER_TOTAL = 6464145.36

RUNNING_COLUMNS = ["c", "oid", "running"]
RUNNING_ROWS = [(c, oid, running) for c, oid, _, running, _, _ in ORDERS]
FILTERED_RUNNING_ROWS = [
    (1, 3868359, 123076.84),
    (1, 5133509, 297722.78),
    (2, 1071617, 221397.35),
    (2, 1374019, 388413.96),
    (2, 1842406, 562705.37),
    (2, 2992930, 666003.05),
    (2, 3986496, 978695.27),
]
NEIGHBOR_COLUMNS = ["c", "oid", "prev_oid", "next_oid"]
NEIGHBOR_ROWS = [(c, oid, prev, nxt) for c, oid, _, _, prev, nxt in ORDERS]
MULTI_COLUMNS = ["c", "oid", "running", "prev_oid"]
MULTI_ROWS = [(c, oid, running, prev) for c, oid, _, running, prev, _ in ORDERS]

# The per-nation test deliberately includes multiple customers in a nation.
# Store the expected nation total once, followed by that nation's order IDs.
NATION_ORDERS = [
    (
        1064401.16,
        [
            491620,
            1521157,
            1932643,
            1967460,
            3051136,
            3938022,
            4114983,
            5414466,
            5534435,
            5566567,
        ],
    ),
    (
        3036045.54,
        [
            9154,
            224167,
            287619,
            869574,
            905633,
            1603585,
            1718016,
            1953381,
            1991461,
            2248546,
            2630562,
            2651266,
            2958117,
            3378979,
            3540450,
            3554659,
            4129186,
            4243392,
            4764293,
            5366593,
            5693440,
            5880160,
        ],
    ),
    (
        2648536.79,
        [
            164711,
            385825,
            1192231,
            1226497,
            1590469,
            1755398,
            1944711,
            1953441,
            1978756,
            2459619,
            2765152,
            2986913,
            3251169,
            3421092,
            3683623,
            3951331,
            4320612,
            4960614,
            5453440,
            5612065,
        ],
    ),
    (
        3039585.48,
        [
            36422,
            816323,
            859108,
            883557,
            895172,
            916775,
            1490087,
            1774689,
            2126688,
            2917345,
            3069221,
            3354726,
            3487745,
            3580034,
            3916288,
            3942656,
            4141668,
            4243142,
            4407621,
            4616224,
        ],
    ),
    (1028273.43, [430243, 1071617, 1374019, 1763205, 1842406, 2992930, 3986496]),
    (587762.91, [454791, 579908, 3868359, 4273923, 4808192, 5133509]),
    (
        1634983.7,
        [
            1073670,
            1201223,
            1859778,
            1925447,
            2415204,
            3692388,
            4208674,
            4225824,
            4550145,
            4659813,
            5091364,
            5805349,
            5987111,
        ],
    ),
    (
        2957861.16,
        [
            135943,
            1485505,
            1894087,
            1959075,
            2014848,
            2096544,
            2943521,
            3038880,
            3211909,
            3318789,
            3431909,
            3995111,
            4163013,
            4328998,
            4860421,
            5090183,
        ],
    ),
    (
        2404167.0,
        [
            212870,
            306439,
            835173,
            1406820,
            2073893,
            2290245,
            2420194,
            3154370,
            3783492,
            4204384,
            4569348,
            5692738,
            5885190,
        ],
    ),
]


def cross_case(
    query: Callable[[], UnqualifiedNode], expected: Callable[[], pd.DataFrame]
):
    """Register a query and its expected frame under the existing snapshot name."""
    name = "cross_tpch_" + query.__name__.removeprefix("cross_")
    return pytest.param(
        PyDoughPandasTest(query, "TPCH", expected, name, fix_column_names=False),
        id=name,
    )


@pytest.fixture(
    params=[
        cross_case(
            queries.cross_top,
            lambda: pd.DataFrame(RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_crossed,
            lambda: pd.DataFrame(RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_reversed_crossed,
            lambda: pd.DataFrame(RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_top2,
            lambda: pd.DataFrame(RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_crossed2,
            lambda: pd.DataFrame(RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_top_neighbors,
            lambda: pd.DataFrame(NEIGHBOR_ROWS, columns=NEIGHBOR_COLUMNS),
        ),
        cross_case(
            queries.cross_crossed_neighbors,
            lambda: pd.DataFrame(NEIGHBOR_ROWS, columns=NEIGHBOR_COLUMNS),
        ),
        cross_case(
            queries.cross_unfiltered_neighbors,
            lambda: pd.DataFrame(NEIGHBOR_ROWS * 2, columns=NEIGHBOR_COLUMNS),
        ),
        cross_case(
            queries.cross_post_cross_window,
            lambda: pd.DataFrame(
                [(c, oid, CROSS_TOTAL) for c, oid, *_ in ORDERS] * 2,
                columns=["c", "oid", "cross_total"],
            ),
        ),
        cross_case(
            queries.cross_filtered_top,
            lambda: pd.DataFrame(FILTERED_RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_filtered_crossed,
            lambda: pd.DataFrame(FILTERED_RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_mixed_windows,
            lambda: pd.DataFrame(
                [(*row, CROSS_TOTAL) for row in RUNNING_ROWS] * 2,
                columns=[*RUNNING_COLUMNS, "cross_total"],
            ),
        ),
        cross_case(
            queries.cross_top_multi,
            lambda: pd.DataFrame(MULTI_ROWS, columns=MULTI_COLUMNS),
        ),
        cross_case(
            queries.cross_crossed_multi,
            lambda: pd.DataFrame(MULTI_ROWS, columns=MULTI_COLUMNS),
        ),
        cross_case(
            queries.cross_nested_partition_windows,
            lambda: pd.DataFrame(
                [
                    (r, k, *row, CROSS_TOTAL, OUTER_TOTAL)
                    for r in REGION_KEYS
                    for k in CUSTOMER_KEYS
                    for row in RUNNING_ROWS
                ],
                columns=["r", "k", *RUNNING_COLUMNS, "inner_total", "outer_total"],
            ),
        ),
        cross_case(
            queries.cross_without_windows,
            lambda: pd.DataFrame(
                [(c, oid, amount) for c, oid, amount, *_ in ORDERS] * 2,
                columns=["c", "oid", "amount"],
            ),
        ),
        cross_case(
            queries.cross_correlated_partition_source,
            lambda: pd.DataFrame(
                [(1, *row) for row in FILTERED_RUNNING_ROWS]
                + [(2, 2, 1071617, 221397.35), (2, 2, 3986496, 534089.57)],
                columns=["k", *RUNNING_COLUMNS],
            ),
        ),
        cross_case(
            queries.cross_rhs_window_no_per,
            lambda: pd.DataFrame(
                [
                    (k, oid, rank, RHS_TOTAL)
                    for k in CUSTOMER_KEYS
                    for rank, oid in enumerate(ORDER_IDS, start=1)
                ],
                columns=["k", "oid", "r", "tot"],
            ),
        ),
        cross_case(
            queries.cross_rhs_window_per_nation,
            lambda: pd.DataFrame(
                [
                    (k, oid, total)
                    for k in CUSTOMER_KEYS
                    for total, order_ids in NATION_ORDERS
                    for oid in order_ids
                ],
                columns=["k", "oid", "s"],
            ),
        ),
        cross_case(
            queries.cross_rhs_window_per_customer,
            lambda: pd.DataFrame(
                [
                    (nk, oid, CUSTOMER_TOTALS[c])
                    for nk in NATION_KEYS
                    for c, oid, *_ in ORDERS
                ],
                columns=["nk", "oid", "s"],
            ),
        ),
        cross_case(
            queries.cross_rhs_partition_aggregate,
            lambda: pd.DataFrame(
                [
                    (c, {1: 6, 2: 7}[c], oid, running)
                    for c, oid, running in RUNNING_ROWS
                ],
                columns=["c", "n", "oid", "running"],
            ),
        ),
        cross_case(
            queries.cross_rhs_lhs_filter_after_window,
            lambda: pd.DataFrame(RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_rhs_lhs_filter_before_window,
            lambda: pd.DataFrame(RUNNING_ROWS, columns=RUNNING_COLUMNS),
        ),
        cross_case(
            queries.cross_rhs_filter_before_global_window,
            lambda: pd.DataFrame(
                [(c, oid, CUSTOMER_TOTALS[c]) for c, oid, *_ in ORDERS],
                columns=["k", "oid", "total"],
            ),
        ),
        cross_case(
            queries.cross_rhs_filter_after_global_window,
            lambda: pd.DataFrame(
                [(c, oid, RHS_TOTAL) for c, oid, *_ in ORDERS],
                columns=["k", "oid", "total"],
            ),
        ),
        cross_case(
            queries.cross_rhs_top_k_before_window,
            lambda: pd.DataFrame(
                [(k, oid, 107685.64) for k in CUSTOMER_KEYS for oid in ORDER_IDS[:2]],
                columns=["k", "oid", "tot"],
            ),
        ),
        cross_case(
            queries.cross_rhs_top_k_after_window,
            lambda: pd.DataFrame(
                [(k, oid, RHS_TOTAL) for k in CUSTOMER_KEYS for oid in ORDER_IDS[:2]],
                columns=["k", "oid", "tot"],
            ),
        ),
        cross_case(
            queries.cross_nested_global_windows,
            lambda: pd.DataFrame(
                [
                    (rk, k, oid, RHS_TOTAL, CROSS_TOTAL, OUTER_TOTAL)
                    for rk in REGION_KEYS
                    for k in CUSTOMER_KEYS
                    for oid in ORDER_IDS
                ],
                columns=["rk", "k", "oid", "t", "inner_total", "outer_total"],
            ),
        ),
        cross_case(
            queries.cross_lhs_has_multiple_levels,
            lambda: pd.DataFrame(
                [
                    (rk, k, oid, RHS_TOTAL)
                    for rk in REGION_KEYS
                    for k in CUSTOMER_KEYS
                    for oid in ORDER_IDS
                ],
                columns=["rk", "k", "oid", "t"],
            ),
        ),
        cross_case(
            queries.cross_window_inside_aggregate,
            lambda: pd.DataFrame(
                [(1, 42016944.84), (2, 42016944.84)], columns=["k", "total"]
            ),
        ),
        cross_case(
            queries.cross_window_in_rhs_child_subtree,
            lambda: pd.DataFrame(
                [
                    (nk, ck, total)
                    for nk in NATION_KEYS
                    for ck, total in [(1, 3526577.46), (2, 7197914.01)]
                ],
                columns=["nk", "ck", "total"],
            ),
        ),
        cross_case(
            queries.cross_explicit_per_after_join,
            lambda: pd.DataFrame(
                [
                    (k, c, oid, 2 * CUSTOMER_TOTALS[c])
                    for k in CUSTOMER_KEYS
                    for c, oid, *_ in ORDERS
                ],
                columns=["k", "c", "oid", "total"],
            ),
        ),
        cross_case(
            queries.cross_rhs_per_lhs_ancestor,
            lambda: pd.DataFrame(
                [
                    (1, nk, oid, 2 * len(ORDER_IDS), CROSS_TOTAL)
                    for nk in (1, 2)
                    for oid in ORDER_IDS
                ],
                columns=["rk", "nk", "oid", "n", "total"],
            ),
        ),
        cross_case(
            queries.cross_nested_per_inner_lhs,
            lambda: pd.DataFrame(
                [
                    (rk, k, oid, RHS_TOTAL)
                    for rk in REGION_KEYS
                    for k in CUSTOMER_KEYS
                    for oid in ORDER_IDS
                ],
                columns=["rk", "k", "oid", "t"],
            ),
        ),
        cross_case(
            queries.cross_range_lhs,
            lambda: pd.DataFrame(
                [
                    (i, oid, rank, RHS_TOTAL)
                    for i in (0, 1)
                    for rank, oid in enumerate(ORDER_IDS, start=1)
                ],
                columns=["i", "oid", "r", "tot"],
            ),
        ),
    ],
)
def cross_pipeline_test_data(request: pytest.FixtureRequest) -> PyDoughPandasTest:
    return request.param


def test_pipeline_until_sql_tpch(
    cross_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """Compare generated SQLite SQL with the checked-in snapshot."""
    file_path = get_sql_test_filename(
        cross_pipeline_test_data.test_name, sqlite_tpch_db_context.dialect
    )
    cross_pipeline_test_data.run_sql_test(
        get_sample_graph, file_path, update_tests, sqlite_tpch_db_context
    )


@pytest.mark.execute
def test_pipeline_e2e_cross(
    cross_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
) -> None:
    """Compare execution results with the case's expected DataFrame."""
    cross_pipeline_test_data.run_e2e_test(
        get_sample_graph, sqlite_tpch_db_context, rtol=1e-9, atol=0.01
    )


@pytest.mark.execute
@pytest.mark.parametrize(
    "top, crossed",
    [
        pytest.param(queries.cross_top, queries.cross_crossed, id="crossed"),
        pytest.param(queries.cross_top, queries.cross_reversed_crossed, id="reversed"),
        pytest.param(queries.cross_top2, queries.cross_crossed2, id="crossed2"),
        pytest.param(
            queries.cross_top_neighbors, queries.cross_crossed_neighbors, id="neighbors"
        ),
        pytest.param(
            queries.cross_filtered_top, queries.cross_filtered_crossed, id="filtered"
        ),
        pytest.param(queries.cross_top_multi, queries.cross_crossed_multi, id="multi"),
    ],
)
def test_cross_matches_standalone(
    top: Callable[[], UnqualifiedNode],
    crossed: Callable[[], UnqualifiedNode],
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
) -> None:
    """Check that adding CROSS and matching the customer preserves window values."""
    graph = get_sample_graph("TPCH")
    session = PyDoughSession()
    session.metadata = graph
    session.database = sqlite_tpch_db_context

    def execute(query: Callable[[], UnqualifiedNode]) -> pd.DataFrame:
        node = transform_and_exec_pydough(query, graph, None, session=session)
        result = to_df(node, session=session)
        return result.sort_values(["c", "oid"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(execute(crossed), execute(top), rtol=1e-9, atol=0.01)
