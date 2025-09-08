"""
Tests the defined behavior of the expressions
used in relational nodes. These are meant to represent
base expressions like column accesses, literals, and
functions.
"""

from typing import Any

import pytest

from pydough.pydough_operators.expression_operators import LOWER, SUM
from pydough.relational import (
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    LiteralExpression,
    RelationalExpression,
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
