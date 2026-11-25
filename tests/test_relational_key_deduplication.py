"""
Unit tests for removing repeated partition and sort keys from relational plans
when they appear during relational optimization, after the expressions or
nodes were built.
"""

import pytest

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughSession
from pydough.conversion.merge_projects import merge_projects
from pydough.conversion.ordering_deduplication import deduplicate_orderings
from pydough.conversion.relational_simplification import (
    PredicateSet,
    SimplificationShuttle,
)
from pydough.relational import (
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    Limit,
    LiteralExpression,
    RelationalExpression,
    RelationalRoot,
    Scan,
    WindowCallExpression,
)
from pydough.relational.rel_util import apply_substitution
from pydough.types import BooleanType, NumericType


def _col(name: str) -> ColumnReference:
    """
    Builds a numeric column reference.
    """
    return ColumnReference(name, NumericType())


def _ranking(partition: list[RelationalExpression]) -> WindowCallExpression:
    """
    Builds a RANKING window expression ordered by column `k` with the given
    partition keys.
    """
    return WindowCallExpression(
        pydop.RANKING,
        NumericType(),
        [],
        partition,
        [ExpressionSortInfo(_col("k"), True, True)],
        {},
    )


def test_substitution_removes_duplicate_partition_keys() -> None:
    """
    Tests that partition keys that become the same expression after a
    substitution are removed when the window expression is rebuilt.
    """
    window: WindowCallExpression = _ranking([_col("c1"), _col("c2"), _col("d")])
    assert len(window.partition_inputs) == 3
    result: RelationalExpression = apply_substitution(
        window, {_col("c1"): _col("c"), _col("c2"): _col("c")}, {}
    )
    assert isinstance(result, WindowCallExpression)
    assert result.partition_inputs == [_col("c"), _col("d")]


def test_substitution_removes_duplicate_window_order_keys() -> None:
    """
    Tests that sort keys that become the same expression after a substitution
    are removed when the window expression is rebuilt.
    """
    window: WindowCallExpression = WindowCallExpression(
        pydop.RANKING,
        NumericType(),
        [],
        [],
        [
            ExpressionSortInfo(_col("k1"), True, True),
            ExpressionSortInfo(_col("k2"), False, False),
        ],
        {},
    )
    result: RelationalExpression = apply_substitution(
        window, {_col("k1"): _col("k"), _col("k2"): _col("k")}, {}
    )
    assert isinstance(result, WindowCallExpression)
    assert result.order_inputs == [ExpressionSortInfo(_col("k"), True, True)]


@pytest.mark.parametrize(
    "partition, output",
    [
        pytest.param(
            [_col("a"), _col("b")],
            [_col("a"), _col("b")],
            id="nothing_simplified",
        ),
        pytest.param(
            [
                _col("a"),
                CallExpression(
                    pydop.IFF,
                    NumericType(),
                    [LiteralExpression(True, BooleanType()), _col("a"), _col("b")],
                ),
            ],
            [_col("a")],
            id="simplified_to_duplicate",
        ),
    ],
)
def test_simplification_removes_duplicate_partition_keys(
    partition: list[RelationalExpression], output: list[RelationalExpression]
) -> None:
    """
    Tests that partition keys that become the same expression after
    simplification are removed, and that the simplifier's stack is left with
    exactly one entry for the window expression.
    """
    shuttle: SimplificationShuttle = SimplificationShuttle(PyDoughSession())
    shuttle.reset()
    result: RelationalExpression = _ranking(partition).accept_shuttle(shuttle)
    assert isinstance(result, WindowCallExpression)
    assert result.partition_inputs == output
    assert len(shuttle.stack) == 1


def test_window_duplicate_keys_after_simplification() -> None:
    """
    Tests that simplification removes repeated partition and ordering keys
    without losing the predicates of the window function arguments.
    """
    a: ColumnReference = _col("a")
    amount: ColumnReference = _col("amount")
    one: LiteralExpression = LiteralExpression(1, NumericType())
    multiplied: CallExpression = CallExpression(pydop.MUL, NumericType(), [a, one])
    window: WindowCallExpression = WindowCallExpression(
        pydop.RELSUM,
        NumericType(),
        [amount],
        [a, multiplied],
        [
            ExpressionSortInfo(a, True, True),
            ExpressionSortInfo(multiplied, True, True),
        ],
        {},
    )
    shuttle: SimplificationShuttle = SimplificationShuttle(PyDoughSession())
    shuttle.input_predicates = {amount: PredicateSet(not_null=True, positive=True)}
    result: RelationalExpression = window.accept_shuttle(shuttle)

    assert isinstance(result, WindowCallExpression)
    assert result.partition_inputs == [a]
    assert result.order_inputs == [ExpressionSortInfo(a, True, True)]
    assert result.inputs == [amount]
    assert len(shuttle.stack) == 1
    assert shuttle.stack[0].not_null
    assert shuttle.stack[0].positive


def test_deduplicate_orderings_after_in_place_rewrites() -> None:
    """
    Tests that the final pass removes repeated sort keys from root and limit
    orderings that were rewritten in place after the nodes were built.
    """
    scan: Scan = Scan("table", {"a": _col("a"), "b": _col("b"), "c": _col("c")})
    limit: Limit = Limit(
        scan,
        LiteralExpression(5, NumericType()),
        {"a": _col("a"), "b": _col("b")},
        [
            ExpressionSortInfo(_col("a"), True, True),
            ExpressionSortInfo(_col("b"), True, True),
        ],
    )
    root: RelationalRoot = RelationalRoot(
        limit,
        [("a", _col("a")), ("b", _col("b"))],
        [
            ExpressionSortInfo(_col("a"), True, True),
            ExpressionSortInfo(_col("c"), True, True),
        ],
    )
    # Simulate passes that rewrite sort keys in place, as projection merging
    # and expression simplification do.
    limit.orderings[1].expr = _col("a")
    root.orderings[1].expr = _col("a")
    deduplicate_orderings(root)
    assert root.orderings == [ExpressionSortInfo(_col("a"), True, True)]
    assert limit.orderings == [ExpressionSortInfo(_col("a"), True, True)]


@pytest.mark.parametrize(
    "root_literal, limit_literal, merged",
    [
        pytest.param(1, 1, True, id="same_literal_type"),
        pytest.param(1, 1.0, False, id="different_literal_type"),
    ],
)
def test_limit_merged_into_root_only_with_same_orderings(
    root_literal: object, limit_literal: object, merged: bool
) -> None:
    """
    Tests that a limit is merged into the root only when both are ordered by
    the same keys. `x + 1` and `x + 1.0` compare equal but can order large
    integers differently, so merging them could change which rows the limit
    keeps.
    """
    x: ColumnReference = _col("x")
    limit: Limit = Limit(
        Scan("t", {"x": x}),
        LiteralExpression(1, NumericType()),
        {"x": x},
        [
            ExpressionSortInfo(
                CallExpression(
                    pydop.ADD,
                    NumericType(),
                    [x, LiteralExpression(limit_literal, NumericType())],
                ),
                True,
                True,
            )
        ],
    )
    root: RelationalRoot = RelationalRoot(
        limit,
        [("x", x)],
        [
            ExpressionSortInfo(
                CallExpression(
                    pydop.ADD,
                    NumericType(),
                    [x, LiteralExpression(root_literal, NumericType())],
                ),
                True,
                True,
            )
        ],
    )
    result = merge_projects(root)
    assert isinstance(result, RelationalRoot)
    assert isinstance(result.input, Limit) != merged
