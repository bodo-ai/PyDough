"""
Tests the defined behavior of the expressions
used in relational nodes. These are meant to represent
base expressions like column accesses, literals, and
functions.
"""

from typing import Any

import pytest

from pydough.pydough_operators.expression_operators import (
    ADD,
    LOWER,
    PREV,
    RANKING,
    SUM,
)
from pydough.relational import (
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    LiteralExpression,
    RelationalExpression,
    WindowCallExpression,
)
from pydough.types import NumericType, StringType, UnknownType


@pytest.mark.parametrize(
    "column_ref, output",
    [
        pytest.param(
            ColumnReference("a", NumericType()),
            "Column(name=a, type=NumericType())",
            id="int_column",
        ),
        pytest.param(
            ColumnReference("b", StringType()),
            "Column(name=b, type=StringType())",
            id="string_column",
        ),
        pytest.param(
            ColumnReference("b", StringType(), "left"),
            "Column(input=left, name=b, type=StringType())",
            id="input_name",
        ),
    ],
)
def test_column_reference_to_string(column_ref: ColumnReference, output: str) -> None:
    """
    Tests the to_string() method of the ColumnReference class.
    """
    assert column_ref.to_string() == output


@pytest.mark.parametrize(
    "ref1, ref2, output",
    [
        pytest.param(
            ColumnReference("a", NumericType()),
            ColumnReference("a", NumericType()),
            True,
            id="same_column",
        ),
        pytest.param(
            ColumnReference("a", NumericType()),
            ColumnReference("b", NumericType()),
            False,
            id="different_name",
        ),
        pytest.param(
            ColumnReference("a", NumericType()),
            ColumnReference("a", StringType()),
            False,
            id="different_type",
        ),
        pytest.param(
            ColumnReference("a", NumericType()),
            LiteralExpression(1, NumericType()),
            False,
            id="different_expr",
        ),
        pytest.param(
            ColumnReference("a", NumericType(), "left"),
            ColumnReference("a", NumericType(), "right"),
            False,
            id="different_input_name",
        ),
        pytest.param(
            ColumnReference("a", NumericType(), "left"),
            ColumnReference("a", NumericType()),
            False,
            id="one_input_name",
        ),
    ],
)
def test_column_reference_equals(
    ref1: ColumnReference, ref2: RelationalExpression, output: bool
) -> None:
    """
    Tests the equality behavior of a ColumnReference with
    another RelationalExpression.
    """
    assert ref1.equals(ref2) == output


@pytest.mark.parametrize(
    "literal, output",
    [
        pytest.param(
            LiteralExpression(1, NumericType()),
            "Literal(value=1, type=NumericType())",
            id="int_literal",
        ),
        pytest.param(
            LiteralExpression("b", StringType()),
            "Literal(value='b', type=StringType())",
            id="string_literal",
        ),
    ],
)
def test_literal_expression_to_string(literal: LiteralExpression, output: str) -> None:
    """
    Tests the to_string() method of the LiteralExpression class.
    """
    assert literal.to_string() == output


@pytest.mark.parametrize(
    "ref1, ref2, output",
    [
        pytest.param(
            LiteralExpression(1, NumericType()),
            LiteralExpression(1, NumericType()),
            True,
            id="same_literal",
        ),
        pytest.param(
            LiteralExpression(1, NumericType()),
            LiteralExpression(2, NumericType()),
            False,
            id="different_value",
        ),
        pytest.param(
            LiteralExpression(1, NumericType()),
            LiteralExpression(1, UnknownType()),
            False,
            id="different_type",
        ),
        pytest.param(
            LiteralExpression(1, NumericType()),
            ColumnReference("a", NumericType()),
            False,
            id="different_expr",
        ),
    ],
)
def test_literals_equal(
    ref1: LiteralExpression, ref2: RelationalExpression, output: bool
) -> None:
    """
    Tests the equality behavior of a LiteralExpression with
    another RelationalExpression.
    """
    assert ref1.equals(ref2) == output


@pytest.mark.parametrize(
    "ordering, output",
    [
        pytest.param(
            ExpressionSortInfo(ColumnReference("a", NumericType()), True, True),
            "ExpressionSortInfo(expression=Column(name=a, type=NumericType()), ascending=True, nulls_first=True)",
            id="asc_nulls_first",
        ),
        pytest.param(
            ExpressionSortInfo(ColumnReference("b", NumericType()), True, False),
            "ExpressionSortInfo(expression=Column(name=b, type=NumericType()), ascending=True, nulls_first=False)",
            id="asc_nulls_last",
        ),
        pytest.param(
            ExpressionSortInfo(ColumnReference("c", NumericType()), False, True),
            "ExpressionSortInfo(expression=Column(name=c, type=NumericType()), ascending=False, nulls_first=True)",
            id="desc_nulls_first",
        ),
        pytest.param(
            ExpressionSortInfo(ColumnReference("d", NumericType()), False, False),
            "ExpressionSortInfo(expression=Column(name=d, type=NumericType()), ascending=False, nulls_first=False)",
            id="desc_nulls_last",
        ),
    ],
)
def test_expression_sort_info_to_string(
    ordering: ExpressionSortInfo, output: str
) -> None:
    """
    Tests the to_string() method of the ExpressionSortInfo class.
    """
    assert ordering.to_string() == output


@pytest.mark.parametrize(
    "ordering1, ordering2, output",
    [
        pytest.param(
            ExpressionSortInfo(ColumnReference("a", NumericType()), True, True),
            ExpressionSortInfo(ColumnReference("a", NumericType()), True, True),
            True,
            id="same_ordering",
        ),
        pytest.param(
            ExpressionSortInfo(ColumnReference("a", NumericType()), True, True),
            ExpressionSortInfo(ColumnReference("b", NumericType()), True, True),
            False,
            id="different_column",
        ),
        pytest.param(
            ExpressionSortInfo(ColumnReference("a", NumericType()), True, True),
            ExpressionSortInfo(ColumnReference("a", NumericType()), False, True),
            False,
            id="different_asc",
        ),
        pytest.param(
            ExpressionSortInfo(ColumnReference("a", NumericType()), True, True),
            ExpressionSortInfo(ColumnReference("a", NumericType()), True, False),
            False,
            id="different_nulls_first",
        ),
        pytest.param(
            ExpressionSortInfo(ColumnReference("a", NumericType()), True, True),
            LiteralExpression(1, NumericType()),
            False,
            id="different_nodes",
        ),
    ],
)
def test_expression_sort_info_equals(
    ordering1: ExpressionSortInfo, ordering2: Any, output: bool
) -> None:
    """
    Tests the equality behavior of a ExpressionSortInfo with
    another object.
    """
    assert (ordering1 == ordering2) == output


@pytest.mark.parametrize(
    "expr, output",
    [
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            "Call(op=LOWER, inputs=[Column(name=a, type=StringType())], return_type=StringType())",
            id="lower",
        ),
        pytest.param(
            CallExpression(SUM, NumericType(), [ColumnReference("a", NumericType())]),
            "Call(op=SUM, inputs=[Column(name=a, type=NumericType())], return_type=NumericType())",
            id="sum",
        ),
    ],
)
def test_call_expressions_to_string(expr: CallExpression, output: str) -> None:
    """
    Tests the to_string() method of the CallExpression class.
    """
    assert expr.to_string() == output


@pytest.mark.parametrize(
    "expr1, expr2, output",
    [
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            True,
            id="same_call",
        ),
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            CallExpression(LOWER, StringType(), [ColumnReference("b", StringType())]),
            False,
            id="different_column",
        ),
        pytest.param(
            CallExpression(SUM, NumericType(), [ColumnReference("a", NumericType())]),
            CallExpression(LOWER, NumericType(), [ColumnReference("a", NumericType())]),
            False,
            id="different_arg_type",
        ),
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            CallExpression(SUM, NumericType(), [ColumnReference("a", StringType())]),
            False,
            id="different_op",
        ),
        pytest.param(
            CallExpression(SUM, NumericType(), [ColumnReference("a", NumericType())]),
            CallExpression(SUM, UnknownType(), [ColumnReference("a", NumericType())]),
            False,
            id="different_return_type",
        ),
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            LiteralExpression(1, NumericType()),
            False,
            id="different_expr",
        ),
    ],
)
def test_call_expressions_equal(
    expr1: CallExpression, expr2: RelationalExpression, output: bool
) -> None:
    """
    Tests the equality behavior of a CallExpression with another
    RelationalExpression.
    """
    assert expr1.equals(expr2) == output


@pytest.mark.parametrize(
    "expr, output",
    [
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            False,
            id="lower",
        ),
        pytest.param(
            CallExpression(SUM, NumericType(), [ColumnReference("a", NumericType())]),
            True,
            id="sum",
        ),
    ],
)
def test_call_expression_is_aggregation(expr: CallExpression, output: bool) -> None:
    """
    Tests the is_aggregation property of the CallExpression class.
    """
    assert expr.is_aggregation == output


def _col(name: str, input_name: str | None = None) -> ColumnReference:
    """
    Builds a numeric column reference for the deduplication tests.
    """
    return ColumnReference(name, NumericType(), input_name)


def _window(
    partition: list[RelationalExpression],
    order: list[ExpressionSortInfo],
    kwargs: dict[str, object] | None = None,
) -> WindowCallExpression:
    """
    Builds a RANKING window expression for the deduplication tests.
    """
    return WindowCallExpression(
        RANKING, NumericType(), [], partition, order, {} if kwargs is None else kwargs
    )


@pytest.mark.parametrize(
    "orderings, output",
    [
        pytest.param([], [], id="empty"),
        pytest.param(
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(_col("b"), False, False),
            ],
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(_col("b"), False, False),
            ],
            id="already_unique",
        ),
        pytest.param(
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(_col("a"), True, True),
            ],
            [ExpressionSortInfo(_col("a"), True, True)],
            id="same_key_twice",
        ),
        pytest.param(
            [
                ExpressionSortInfo(_col("a"), False, False),
                ExpressionSortInfo(_col("a"), True, True),
            ],
            [ExpressionSortInfo(_col("a"), False, False)],
            id="different_direction_keeps_first",
        ),
        pytest.param(
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(_col("a"), True, False),
            ],
            [ExpressionSortInfo(_col("a"), True, True)],
            id="different_nulls_keeps_first",
        ),
        pytest.param(
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(_col("b"), True, True),
                ExpressionSortInfo(_col("a"), False, True),
                ExpressionSortInfo(_col("c"), True, True),
                ExpressionSortInfo(_col("b"), True, True),
            ],
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(_col("b"), True, True),
                ExpressionSortInfo(_col("c"), True, True),
            ],
            id="preserves_order",
        ),
        pytest.param(
            [
                ExpressionSortInfo(_col("a", "t0"), True, True),
                ExpressionSortInfo(_col("a", "t1"), True, True),
            ],
            [
                ExpressionSortInfo(_col("a", "t0"), True, True),
                ExpressionSortInfo(_col("a", "t1"), True, True),
            ],
            id="same_name_different_input",
        ),
        pytest.param(
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(ColumnReference("a", StringType()), True, True),
            ],
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(ColumnReference("a", StringType()), True, True),
            ],
            id="same_name_different_type",
        ),
        pytest.param(
            [
                ExpressionSortInfo(
                    _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
                    True,
                    True,
                ),
                ExpressionSortInfo(
                    _window([_col("c")], [ExpressionSortInfo(_col("b"), True, True)]),
                    True,
                    True,
                ),
            ],
            [
                ExpressionSortInfo(
                    _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
                    True,
                    True,
                ),
                ExpressionSortInfo(
                    _window([_col("c")], [ExpressionSortInfo(_col("b"), True, True)]),
                    True,
                    True,
                ),
            ],
            id="windows_with_different_partitions",
        ),
    ],
)
def test_expression_sort_info_deduplicate(
    orderings: list[ExpressionSortInfo], output: list[ExpressionSortInfo]
) -> None:
    """
    Tests that ExpressionSortInfo.deduplicate removes repeated sort
    expressions, keeping the first occurrence and the original order.
    """
    assert ExpressionSortInfo.deduplicate(orderings) == output


@pytest.mark.parametrize(
    "partition, output",
    [
        pytest.param([], [], id="empty"),
        pytest.param(
            [_col("a"), _col("b")], [_col("a"), _col("b")], id="already_unique"
        ),
        pytest.param([_col("a"), _col("a")], [_col("a")], id="same_key_twice"),
        pytest.param(
            [_col("b"), _col("a"), _col("b"), _col("c"), _col("a")],
            [_col("b"), _col("a"), _col("c")],
            id="preserves_order",
        ),
        pytest.param(
            [_col("a", "t0"), _col("a", "t1")],
            [_col("a", "t0"), _col("a", "t1")],
            id="same_name_different_input",
        ),
        pytest.param(
            [
                CallExpression(
                    LOWER, StringType(), [ColumnReference("s", StringType())]
                ),
                CallExpression(
                    LOWER, StringType(), [ColumnReference("s", StringType())]
                ),
            ],
            [CallExpression(LOWER, StringType(), [ColumnReference("s", StringType())])],
            id="same_computed_expression",
        ),
        pytest.param(
            [
                CallExpression(
                    ADD, NumericType(), [_col("x"), LiteralExpression(1, NumericType())]
                ),
                CallExpression(
                    ADD, NumericType(), [_col("x"), LiteralExpression(1, NumericType())]
                ),
            ],
            [
                CallExpression(
                    ADD, NumericType(), [_col("x"), LiteralExpression(1, NumericType())]
                )
            ],
            id="same_expression_with_literal",
        ),
        pytest.param(
            [
                _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
                _window([_col("c")], [ExpressionSortInfo(_col("b"), True, True)]),
            ],
            [
                _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
                _window([_col("c")], [ExpressionSortInfo(_col("b"), True, True)]),
            ],
            id="windows_with_different_partitions",
        ),
    ],
)
def test_window_call_expression_partition_deduplication(
    partition: list[RelationalExpression], output: list[RelationalExpression]
) -> None:
    """
    Tests that WindowCallExpression removes repeated partition keys when it is
    built, keeping the first occurrence and the original order.
    """
    window: WindowCallExpression = WindowCallExpression(
        PREV, NumericType(), [_col("x")], partition, [], {"n": 1}
    )
    assert window.partition_inputs == output
    assert len(window.partition_inputs) == len(output)


@pytest.mark.parametrize(
    "order, output",
    [
        pytest.param([], [], id="empty"),
        pytest.param(
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(_col("b"), True, True),
            ],
            [
                ExpressionSortInfo(_col("a"), True, True),
                ExpressionSortInfo(_col("b"), True, True),
            ],
            id="already_unique",
        ),
        pytest.param(
            [
                ExpressionSortInfo(_col("a"), True, False),
                ExpressionSortInfo(_col("a"), False, True),
                ExpressionSortInfo(_col("b"), True, True),
            ],
            [
                ExpressionSortInfo(_col("a"), True, False),
                ExpressionSortInfo(_col("b"), True, True),
            ],
            id="repeated_key_any_direction",
        ),
    ],
)
def test_window_call_expression_order_deduplication(
    order: list[ExpressionSortInfo], output: list[ExpressionSortInfo]
) -> None:
    """
    Tests that WindowCallExpression removes repeated sort keys when it is
    built, keeping the first occurrence and the original order.
    """
    assert _window([], order).order_inputs == output


@pytest.mark.parametrize(
    "window1, window2, output",
    [
        pytest.param(
            _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
            _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
            True,
            id="same_window",
        ),
        pytest.param(
            _window(
                [_col("a"), _col("a")], [ExpressionSortInfo(_col("b"), True, True)]
            ),
            _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
            True,
            id="equal_after_deduplication",
        ),
        pytest.param(
            _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
            _window([_col("c")], [ExpressionSortInfo(_col("b"), True, True)]),
            False,
            id="different_partition",
        ),
        pytest.param(
            _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
            _window([_col("a")], [ExpressionSortInfo(_col("c"), True, True)]),
            False,
            id="different_order_key",
        ),
        pytest.param(
            _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
            _window([_col("a")], [ExpressionSortInfo(_col("b"), False, True)]),
            False,
            id="different_order_direction",
        ),
        pytest.param(
            _window(
                [_col("a")],
                [ExpressionSortInfo(_col("b"), True, True)],
                {"allow_ties": True},
            ),
            _window([_col("a")], [ExpressionSortInfo(_col("b"), True, True)]),
            False,
            id="different_kwargs",
        ),
        pytest.param(
            _window(
                [_col("a")],
                [ExpressionSortInfo(_col("b"), True, True)],
                {"allow_ties": True, "dense": False},
            ),
            _window(
                [_col("a")],
                [ExpressionSortInfo(_col("b"), True, True)],
                {"dense": False, "allow_ties": True},
            ),
            True,
            id="same_kwargs_different_insertion_order",
        ),
        pytest.param(
            _window(
                [_col("a")], [ExpressionSortInfo(_col("b"), True, True)], {"default": 0}
            ),
            _window(
                [_col("a")],
                [ExpressionSortInfo(_col("b"), True, True)],
                {"default": 0.0},
            ),
            False,
            id="kwarg_values_equal_but_different_type",
        ),
        pytest.param(
            _window(
                [_col("a")],
                [ExpressionSortInfo(_col("b"), True, True)],
                {"frame": (1, None)},
            ),
            _window(
                [_col("a")],
                [ExpressionSortInfo(_col("b"), True, True)],
                {"frame": (1.0, None)},
            ),
            False,
            id="nested_kwarg_values_equal_but_different_type",
        ),
        pytest.param(
            _window(
                [_col("a")],
                [ExpressionSortInfo(_col("b"), True, True)],
                {"frame": (1, None), "cumulative": False},
            ),
            _window(
                [_col("a")],
                [ExpressionSortInfo(_col("b"), True, True)],
                {"cumulative": False, "frame": (1, None)},
            ),
            True,
            id="same_nested_kwargs_different_insertion_order",
        ),
        pytest.param(
            _window(
                [_col("a")], [ExpressionSortInfo(_col("b"), True, True)], {"x": [1, 2]}
            ),
            _window(
                [_col("a")], [ExpressionSortInfo(_col("b"), True, True)], {"x": [1, 2]}
            ),
            True,
            id="unhashable_kwarg_value",
        ),
    ],
)
def test_window_call_expressions_equal(
    window1: WindowCallExpression, window2: RelationalExpression, output: bool
) -> None:
    """
    Tests that WindowCallExpression equality compares the partition keys,
    sort keys and keyword arguments, and agrees with the hash.
    """
    assert window1.equals(window2) == output
    assert (window1 == window2) == output
    # Equal windows must have equal hashes, so that a dictionary keyed by one
    # of them can be looked up with the other.
    lookup: dict[RelationalExpression, str] = {window1: "found"}
    if output:
        assert hash(window1) == hash(window2)
        assert lookup.get(window2) == "found"
    else:
        assert window2 not in lookup


def test_deduplicate_orderings_preserves_numeric_arithmetic() -> None:
    """
    Tests that calculated ordering expressions with different
    numeric literal types are not incorrectly deduplicated.
    """
    typ = NumericType()
    x = ColumnReference("x", typ)

    real_expr = CallExpression(
        ADD,
        typ,
        [x, LiteralExpression(1.0, typ)],
    )

    integer_expr = CallExpression(
        ADD,
        typ,
        [x, LiteralExpression(1, typ)],
    )

    orderings = [
        ExpressionSortInfo(real_expr, True, True),
        ExpressionSortInfo(integer_expr, False, True),
    ]

    result = ExpressionSortInfo.deduplicate(orderings)

    assert len(result) == 2


def test_window_partition_preserves_numeric_arithmetic() -> None:
    """
    Tests that calculated partition expressions with different numeric
    literal types are not incorrectly deduplicated.
    """
    typ = NumericType()
    x = ColumnReference("x", typ)

    real_expr = CallExpression(
        ADD,
        typ,
        [x, LiteralExpression(1.0, typ)],
    )

    integer_expr = CallExpression(
        ADD,
        typ,
        [x, LiteralExpression(1, typ)],
    )

    window = WindowCallExpression(
        RANKING,
        typ,
        [],
        [real_expr, integer_expr],
        [ExpressionSortInfo(x, True, True)],
        {},
    )

    assert len(window.partition_inputs) == 2
