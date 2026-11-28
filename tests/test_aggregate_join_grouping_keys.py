"""
Function-style TPCH integration tests for aggregate-join transposition (GH-653).
Direct optimizer regression tests are in test_split_partial_aggregates.py.
"""

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
import pytest

from pydough import to_sql
from pydough.configs import PyDoughSession
from pydough.conversion import relational_converter
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.relational import RelationalNode, RelationalRoot
from pydough.relational.relational_nodes.column_pruner import ColumnPruner
from pydough.unqualified import UnqualifiedNode
from tests.test_pydough_functions.aggregate_join_pydough_functions import (
    aggregate_join_nested_two_sided_key,
    aggregate_join_separate_keys,
    aggregate_join_two_sided_key,
)

from .testing_utilities import (
    PyDoughPandasTest,
    graph_fetcher,
    transform_and_exec_pydough,
)


@dataclass(frozen=True)
class AggregateJoinTestCase:
    """A function-style PyDough query and its independent reference SQL."""

    pydough_function: Callable[..., UnqualifiedNode]
    reference_sql: str
    expected_rows: int

    def make_test_data(self, expected: pd.DataFrame | None = None) -> PyDoughPandasTest:
        return PyDoughPandasTest(
            self.pydough_function,
            "TPCH",
            # Plan and SQL snapshot tests do not evaluate the reference function.
            lambda: pd.DataFrame() if expected is None else expected,
            self.pydough_function.__name__,
            order_sensitive=True,
            fix_column_names=False,
        )


@pytest.fixture(
    params=[
        pytest.param(
            AggregateJoinTestCase(
                aggregate_join_two_sided_key,
                """
                SELECT
                    c.c_mktsegment || ' - ' || n.n_name AS label,
                    COUNT(*) AS n
                FROM tpch.customer AS c
                JOIN tpch.nation AS n
                    ON c.c_nationkey = n.n_nationkey
                GROUP BY 1
                ORDER BY 1
                """,
                125,
            ),
            id="aggregate_join_two_sided_key",
        ),
        pytest.param(
            AggregateJoinTestCase(
                aggregate_join_nested_two_sided_key,
                """
                SELECT
                    n.n_name || ' - ' || r.r_name AS label,
                    COUNT(*) AS n_customers
                FROM tpch.customer AS c
                JOIN tpch.nation AS n
                    ON c.c_nationkey = n.n_nationkey
                JOIN tpch.region AS r
                    ON n.n_regionkey = r.r_regionkey
                GROUP BY 1
                ORDER BY 1
                """,
                25,
            ),
            id="aggregate_join_nested_two_sided_key",
        ),
        pytest.param(
            AggregateJoinTestCase(
                aggregate_join_separate_keys,
                """
                SELECT
                    c.c_mktsegment AS market_segment,
                    n.n_name AS nation_name,
                    COUNT(*) AS n
                FROM tpch.customer AS c
                JOIN tpch.nation AS n
                    ON c.c_nationkey = n.n_nationkey
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
                125,
            ),
            id="aggregate_join_separate_keys",
        ),
    ]
)
def aggregate_join_case(request: pytest.FixtureRequest) -> AggregateJoinTestCase:
    return request.param


def enable_extra_pruning(monkeypatch: pytest.MonkeyPatch) -> None:
    """Expose the unsafe optimizer input without changing production code."""
    original_split = relational_converter.split_partial_aggregates

    def split_after_pruning(
        root: RelationalRoot, session: PyDoughSession
    ) -> RelationalNode:
        # Reproduce the sensitivity experiment in #653 by removing redundant
        # projections immediately before partial aggregation is split.
        root = ColumnPruner().prune_unused_columns(root, True)
        return original_split(root, session)

    # Patch the caller's binding, not agg_split.split_partial_aggregates:
    # the additional pruning should run once, not at every recursive call.
    monkeypatch.setattr(
        relational_converter, "split_partial_aggregates", split_after_pruning
    )


def test_pipeline_until_relational_aggregate_join_grouping_keys(
    aggregate_join_case: AggregateJoinTestCase,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """Check the normal production pipeline's Relational plan."""
    test_data = aggregate_join_case.make_test_data()
    test_data.run_relational_test(
        get_sample_graph,
        get_plan_test_filename(test_data.test_name),
        update_tests,
    )


def test_pipeline_until_sql_aggregate_join_grouping_keys(
    aggregate_join_case: AggregateJoinTestCase,
    get_sample_graph: graph_fetcher,
    empty_sqlite_tpch_session: PyDoughSession,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Check SQL generated by the normal pipeline. The fix only affects the
    Relational optimizer, so a single dialect is enough here; dialect-specific
    SQL generation is already covered elsewhere.
    """
    test_data = aggregate_join_case.make_test_data()
    test_data.run_sql_test(
        get_sample_graph,
        get_sql_test_filename(test_data.test_name, DatabaseDialect.SQLITE),
        update_tests,
        empty_sqlite_tpch_session.database,
        config=empty_sqlite_tpch_session.config,
    )


def test_aggregate_join_grouping_keys_extra_pruning_sql(
    aggregate_join_case: AggregateJoinTestCase,
    empty_sqlite_tpch_session: PyDoughSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expose the regression at SQL generation time without a TPCH database."""
    enable_extra_pruning(monkeypatch)
    session = empty_sqlite_tpch_session
    graph = session.metadata
    assert graph is not None
    root = transform_and_exec_pydough(
        aggregate_join_case.pydough_function, graph, None, session=session
    )
    # Before the fix, the two cross-input keys produce invalid plans with
    # missing column references. SQL generation must succeed after the fix.
    sql = to_sql(
        root,
        metadata=graph,
        database=session.database,
        config=session.config,
    )
    assert sql.strip()


@pytest.mark.execute
@pytest.mark.parametrize(
    "extra_pruning",
    [
        pytest.param(False, id="normal"),
        pytest.param(True, id="extra_pruning"),
    ],
)
def test_aggregate_join_grouping_keys_e2e(
    aggregate_join_case: AggregateJoinTestCase,
    extra_pruning: bool,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compare normal and sensitivity-mode results to independent SQL."""
    if extra_pruning:
        enable_extra_pruning(monkeypatch)

    expected = sqlite_tpch_db_context.execute_query_df(
        aggregate_join_case.reference_sql
    )
    assert len(expected) == aggregate_join_case.expected_rows

    aggregate_join_case.make_test_data(expected).run_e2e_test(
        get_sample_graph,
        sqlite_tpch_db_context,
        coerce_types=True,
    )
