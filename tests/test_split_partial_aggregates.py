"""
Direct regression tests for `split_partial_aggregates` (GH-653).

Each case builds an `Aggregate(Join(...))` plan by hand on the TPC-H tables
(CUSTOMER, NATION, REGION), so these tests do not depend on the plan shapes
that earlier optimizer passes produce.

- The plan test applies only `split_partial_aggregates` and asserts which
  join inputs received a partial aggregate, before any column pruning. When
  the split is rejected, it also asserts that the plan is unchanged. It then
  prunes the plan and checks that SQL can still be generated from it.
- The results test runs the original and the split plan on the TPC-H SQLite
  database and checks that they return the same rows.
"""

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
import pytest

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughSession
from pydough.conversion.agg_split import split_partial_aggregates
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    Join,
    JoinType,
    LiteralExpression,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    Scan,
)
from pydough.relational.relational_nodes.column_pruner import ColumnPruner
from pydough.relational.relational_nodes.join import JoinCardinality
from pydough.sqlglot import convert_relation_to_sql, execute_df
from pydough.types import BooleanType, NumericType, PyDoughType, StringType

N: PyDoughType = NumericType()
S: PyDoughType = StringType()
B: PyDoughType = BooleanType()

CUSTOMER: str = "tpch.CUSTOMER"
NATION: str = "tpch.NATION"
REGION: str = "tpch.REGION"


def col(
    name: str, data_type: PyDoughType, input_name: str | None = None
) -> ColumnReference:
    return ColumnReference(name, data_type, input_name)


def concat(*args: RelationalExpression) -> CallExpression:
    return CallExpression(pydop.JOIN_STRINGS, S, [LiteralExpression("-", S), *args])


def agg_call(
    op: pydop.PyDoughExpressionOperator, *args: RelationalExpression
) -> CallExpression:
    return CallExpression(op, N, list(args))


def scan_customer() -> Scan:
    return Scan(
        CUSTOMER,
        {
            "c_nationkey": col("c_nationkey", N),
            "c_mktsegment": col("c_mktsegment", S),
            "c_acctbal": col("c_acctbal", N),
        },
    )


def scan_nation() -> Scan:
    return Scan(
        NATION,
        {
            "n_nationkey": col("n_nationkey", N),
            "n_name": col("n_name", S),
            "n_regionkey": col("n_regionkey", N),
        },
    )


def scan_region() -> Scan:
    return Scan(
        REGION, {"r_regionkey": col("r_regionkey", N), "r_name": col("r_name", S)}
    )


def join_customer_nation(
    columns: dict[str, RelationalExpression],
    join_type: JoinType = JoinType.INNER,
    right: RelationalNode | None = None,
) -> Join:
    """
    `CUSTOMER JOIN <right> ON c_nationkey = n_nationkey`, where `<right>`
    defaults to NATION.
    """
    return Join(
        [scan_customer(), scan_nation() if right is None else right],
        CallExpression(
            pydop.EQU,
            B,
            [col("c_nationkey", N, "t0"), col("n_nationkey", N, "t1")],
        ),
        join_type,
        columns,
        JoinCardinality.SINGULAR_ACCESS,
        JoinCardinality.PLURAL_ACCESS,
    )


def join_nation_region(columns: dict[str, RelationalExpression]) -> Join:
    """`NATION JOIN REGION ON n_regionkey = r_regionkey`."""
    return Join(
        [scan_nation(), scan_region()],
        CallExpression(
            pydop.EQU,
            B,
            [col("n_regionkey", N, "t0"), col("r_regionkey", N, "t1")],
        ),
        JoinType.INNER,
        columns,
        JoinCardinality.SINGULAR_ACCESS,
        JoinCardinality.PLURAL_ACCESS,
    )


def customer_nation_aggregate(
    keys: dict[str, RelationalExpression],
    aggs: dict[str, CallExpression],
    join_type: JoinType = JoinType.INNER,
) -> Aggregate:
    """An aggregate directly above `CUSTOMER JOIN NATION`."""
    columns: dict[str, RelationalExpression] = {
        "c_mktsegment": col("c_mktsegment", S, "t0"),
        "c_acctbal": col("c_acctbal", N, "t0"),
        "n_name": col("n_name", S, "t1"),
        "n_regionkey": col("n_regionkey", N, "t1"),
    }
    return Aggregate(join_customer_nation(columns, join_type), keys, aggs)


def nested_aggregate(key_side: str) -> Aggregate:
    """
    An aggregate above `CUSTOMER JOIN (NATION JOIN REGION)`, grouped by a
    single key:
    - `"inner"`: `n_name || r_name`, which reads only the right input of the
      outer join, but both inputs of the inner join.
    - `"outer"`: `c_mktsegment || r_name`, which reads both inputs of the
      outer join.
    """
    inner: Join = join_nation_region(
        {
            "n_nationkey": col("n_nationkey", N, "t0"),
            "n_name": col("n_name", S, "t0"),
            "r_name": col("r_name", S, "t1"),
        }
    )
    outer: Join = join_customer_nation(
        {
            "c_mktsegment": col("c_mktsegment", S, "t0"),
            "n_name": col("n_name", S, "t1"),
            "r_name": col("r_name", S, "t1"),
        },
        right=inner,
    )
    key: CallExpression = (
        concat(col("n_name", S), col("r_name", S))
        if key_side == "inner"
        else concat(col("c_mktsegment", S), col("r_name", S))
    )
    return Aggregate(outer, {"label": key}, {"n": agg_call(pydop.COUNT)})


C = frozenset({CUSTOMER})
NA = frozenset({NATION})
NA_R = frozenset({NATION, REGION})


def two_sided_key() -> dict[str, RelationalExpression]:
    """Build a fresh grouping expression that references both join inputs."""
    return {"label": concat(col("c_mktsegment", S), col("n_name", S))}


@dataclass(frozen=True)
class RelationalAggJoinCase:
    """
    A hand-built `Aggregate` plan on TPC-H, plus the expected result of
    `split_partial_aggregates` on it.
    """

    build: Callable[[], Aggregate]
    """Builds a fresh plan. The optimization mutates nodes in place."""

    pushed_into: frozenset[frozenset[str]]
    """
    The expected pushed-down aggregates. Each element is the set of tables
    beneath an `Aggregate` that is a direct input to a `Join`. Empty means the
    aggregate was not transposed at all.
    """

    prune: bool = True
    """Whether to run the column pruner after the split, as the pipeline does."""


relational_agg_join_cases = [
    # A single grouping key reads both join inputs: never transposed.
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                two_sided_key(), {"n": agg_call(pydop.COUNT)}
            ),
            frozenset(),
        ),
        id="two_sided_key-count",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                two_sided_key(), {"s": agg_call(pydop.SUM, col("c_acctbal", N))}
            ),
            frozenset(),
        ),
        id="two_sided_key-sum_left",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                two_sided_key(), {"s": agg_call(pydop.SUM, col("n_regionkey", N))}
            ),
            frozenset(),
        ),
        id="two_sided_key-sum_right",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                two_sided_key(),
                {
                    "lo": agg_call(pydop.MIN, col("c_acctbal", N)),
                    "hi": agg_call(pydop.MAX, col("n_regionkey", N)),
                },
            ),
            frozenset(),
        ),
        id="two_sided_key-min_max",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                two_sided_key(), {"m": agg_call(pydop.AVG, col("c_acctbal", N))}
            ),
            frozenset(),
        ),
        id="two_sided_key-avg_left",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                two_sided_key(), {"m": agg_call(pydop.AVG, col("n_regionkey", N))}
            ),
            frozenset(),
        ),
        id="two_sided_key-avg_right",
    ),
    pytest.param(
        # Only the left input is eligible for a LEFT join, and the left
        # aggregate would drop `c_mktsegment`.
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                two_sided_key(),
                {"s": agg_call(pydop.SUM, col("c_acctbal", N))},
                JoinType.LEFT,
            ),
            frozenset(),
        ),
        id="two_sided_key-left_join",
    ),
    pytest.param(
        # Join output names differ from the input column names.
        RelationalAggJoinCase(
            lambda: Aggregate(
                join_customer_nation(
                    {
                        "segment": col("c_mktsegment", S, "t0"),
                        "nation_name": col("n_name", S, "t1"),
                    }
                ),
                {"label": concat(col("segment", S), col("nation_name", S))},
                {"n": agg_call(pydop.COUNT)},
            ),
            frozenset(),
        ),
        id="two_sided_key-aliased_join_columns",
    ),
    pytest.param(
        # The key is a plain reference to a join column that is itself a
        # two-sided expression (the shape `merge_projects` can produce). The
        # column pruner does not support this join shape, so it is skipped.
        RelationalAggJoinCase(
            lambda: Aggregate(
                join_customer_nation(
                    {
                        "label": concat(
                            col("c_mktsegment", S, "t0"), col("n_name", S, "t1")
                        )
                    }
                ),
                {"label": col("label", S)},
                {"n": agg_call(pydop.COUNT)},
            ),
            frozenset(),
            prune=False,
        ),
        id="two_sided_key-expression_join_column",
    ),
    pytest.param(
        # Single-sided at the outer join, so the outer split still happens;
        # two-sided at the inner join, so NATION JOIN REGION is aggregated as
        # a whole.
        RelationalAggJoinCase(lambda: nested_aggregate("inner"), frozenset({C, NA_R})),
        id="nested-two_sided_at_inner_join",
    ),
    pytest.param(
        RelationalAggJoinCase(lambda: nested_aggregate("outer"), frozenset()),
        id="nested-two_sided_at_outer_join",
    ),
    pytest.param(
        # Conservative: `c_mktsegment` is also its own key, so a split would
        # have been valid here, but one key is two-sided and the fix rejects
        # it. Update this case if the check is refined to allow it.
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                {"c_mktsegment": col("c_mktsegment", S), **two_sided_key()},
                {"s": agg_call(pydop.SUM, col("c_acctbal", N))},
            ),
            frozenset(),
        ),
        id="conservative-single_and_two_sided_keys",
    ),
    # Controls: every key reads at most one input, so the split still happens.
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                {"c_mktsegment": col("c_mktsegment", S), "n_name": col("n_name", S)},
                {
                    "s": agg_call(pydop.SUM, col("c_acctbal", N)),
                    "n": agg_call(pydop.COUNT),
                },
            ),
            frozenset({C, NA}),
        ),
        id="control-separate_keys",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                {"c_mktsegment": col("c_mktsegment", S)},
                {"s": agg_call(pydop.SUM, col("n_regionkey", N))},
            ),
            frozenset({NA}),
        ),
        id="control-left_key-sum_right",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                {"n_name": col("n_name", S)},
                {"s": agg_call(pydop.SUM, col("c_acctbal", N))},
            ),
            frozenset({C}),
        ),
        id="control-right_key-sum_left",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                {"label": concat(col("c_mktsegment", S), col("c_mktsegment", S))},
                {"n": agg_call(pydop.COUNT)},
            ),
            frozenset({C, NA}),
        ),
        id="control-calculated_left_key",
    ),
    pytest.param(
        RelationalAggJoinCase(
            lambda: customer_nation_aggregate(
                {"label": LiteralExpression("k", S)}, {"n": agg_call(pydop.COUNT)}
            ),
            frozenset({C, NA}),
        ),
        id="control-literal_key",
    ),
]


def output_columns(aggregate: Aggregate) -> list[tuple[str, RelationalExpression]]:
    """Root columns exposing every output column of `aggregate`."""
    return [
        (name, col(name, expr.data_type)) for name, expr in aggregate.columns.items()
    ]


def split_aggregate(aggregate: Aggregate, session: PyDoughSession) -> RelationalRoot:
    """Apply only the aggregate-splitting pass to a fresh input plan."""
    columns = output_columns(aggregate)
    return RelationalRoot(split_partial_aggregates(aggregate, session), columns)


def prune_after_split(
    root: RelationalRoot, case: RelationalAggJoinCase
) -> RelationalRoot:
    """Apply subsequent pruning where supported by the input plan shape."""
    if case.prune:
        return ColumnPruner().prune_unused_columns(root, True)
    return root


def scanned_tables(node: RelationalNode) -> frozenset[str]:
    tables: set[str] = {node.table_name} if isinstance(node, Scan) else set()
    for child in node.inputs:
        tables |= scanned_tables(child)
    return frozenset(tables)


def pushed_down_aggregates(node: RelationalNode) -> frozenset[frozenset[str]]:
    """
    The tables beneath each `Aggregate` that is a direct input to a `Join`,
    i.e. each aggregate that was pushed beneath a join.
    """
    result: set[frozenset[str]] = set()
    if isinstance(node, Join):
        for join_input in node.inputs:
            if isinstance(join_input, Aggregate):
                result.add(scanned_tables(join_input))
    for child in node.inputs:
        result |= pushed_down_aggregates(child)
    return frozenset(result)


@pytest.mark.parametrize("case", relational_agg_join_cases)
def test_split_partial_aggregates_grouping_keys_plan(
    case: RelationalAggJoinCase, empty_sqlite_tpch_session: PyDoughSession
) -> None:
    """
    Check where `split_partial_aggregates` pushes aggregates for each case, and
    that SQL can be generated from the result (an invalid plan fails with
    `Unknown column`).
    """
    aggregate = case.build()
    original_plan = aggregate.to_tree_string()
    root = split_aggregate(aggregate, empty_sqlite_tpch_session)

    # Assert the split itself, before pruning can change the plan shape.
    assert pushed_down_aggregates(root) == case.pushed_into, root.to_tree_string()
    if not case.pushed_into:
        # The rejected rewrite must leave the entire input tree unchanged.
        # Compare its representation, not a mutable node or its equality method.
        assert root.input.to_tree_string() == original_plan

    # Ensure the resulting plan can still be consumed by later pipeline stages.
    root = prune_after_split(root, case)
    assert convert_relation_to_sql(root, empty_sqlite_tpch_session).strip()


@pytest.mark.execute
@pytest.mark.parametrize("case", relational_agg_join_cases)
def test_split_partial_aggregates_grouping_keys_results(
    case: RelationalAggJoinCase, sqlite_tpch_session: PyDoughSession
) -> None:
    """
    Check on the TPC-H SQLite database that the split plan returns the same
    rows as the original plan.
    """
    aggregate: Aggregate = case.build()
    original: RelationalRoot = RelationalRoot(aggregate, output_columns(aggregate))
    expected: pd.DataFrame = execute_df(original, sqlite_tpch_session)
    split_root = split_aggregate(case.build(), sqlite_tpch_session)
    result: pd.DataFrame = execute_df(
        prune_after_split(split_root, case), sqlite_tpch_session
    )
    # SQLite reports unaliased columns with the table's casing, which can differ
    # between the two plans, so compare column names case-insensitively.
    expected.columns = expected.columns.str.lower()
    result.columns = result.columns.str.lower()
    sort_columns: list[str] = list(expected.columns)
    pd.testing.assert_frame_equal(
        result.sort_values(sort_columns).reset_index(drop=True),
        expected.sort_values(sort_columns).reset_index(drop=True),
        check_dtype=False,
    )
