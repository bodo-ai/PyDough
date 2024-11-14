"""
Tests the defined behavior of the expressions
used in relational nodes. These are meant to represent
base expressions like column accesses, literals, and
functions.
"""

from typing import Any

import pytest

from pydough.pydough_ast.pydough_operators.expression_operators import LOWER, SUM
from pydough.relational.relational_expressions import (
    ColumnOrdering,
    RelationalExpression,
)
from pydough.relational.relational_expressions.call_expression import CallExpression
from pydough.relational.relational_expressions.column_reference import ColumnReference
from pydough.relational.relational_expressions.literal_expression import (
    LiteralExpression,
)
from pydough.types import Int32Type, Int64Type, StringType


@pytest.mark.parametrize(
    "column_ref, output",
    [
        pytest.param(
            ColumnReference("a", Int64Type()),
            "Column(name=a, type=Int64Type())",
            id="int_column",
        ),
        pytest.param(
            ColumnReference("b", StringType()),
            "Column(name=b, type=StringType())",
            id="string_column",
        ),
    ],
)
def test_column_reference_to_string(column_ref: ColumnReference, output: str):
    assert column_ref.to_string() == output


@pytest.mark.parametrize(
    "ref1, ref2, output",
    [
        pytest.param(
            ColumnReference("a", Int64Type()),
            ColumnReference("a", Int64Type()),
            True,
            id="same_column",
        ),
        pytest.param(
            ColumnReference("a", Int64Type()),
            ColumnReference("b", Int64Type()),
            False,
            id="different_name",
        ),
        pytest.param(
            ColumnReference("a", Int64Type()),
            ColumnReference("a", StringType()),
            False,
            id="different_type",
        ),
        pytest.param(
            ColumnReference("a", Int64Type()),
            LiteralExpression(1, Int64Type()),
            False,
            id="different_expr",
        ),
    ],
)
def test_column_reference_equals(
    ref1: ColumnReference, ref2: RelationalExpression, output: bool
):
    assert ref1.equals(ref2) == output


@pytest.mark.parametrize(
    "literal, output",
    [
        pytest.param(
            LiteralExpression(1, Int64Type()),
            "Literal(value=1, type=Int64Type())",
            id="int_literal",
        ),
        pytest.param(
            LiteralExpression("b", StringType()),
            "Literal(value=b, type=StringType())",
            id="string_literal",
        ),
    ],
)
def test_literal_expression_to_string(literal: LiteralExpression, output: str):
    assert literal.to_string() == output


@pytest.mark.parametrize(
    "ref1, ref2, output",
    [
        pytest.param(
            LiteralExpression(1, Int64Type()),
            LiteralExpression(1, Int64Type()),
            True,
            id="same_literal",
        ),
        pytest.param(
            LiteralExpression(1, Int64Type()),
            LiteralExpression(2, Int64Type()),
            False,
            id="different_value",
        ),
        pytest.param(
            LiteralExpression(1, Int64Type()),
            LiteralExpression(1, Int32Type()),
            False,
            id="different_type",
        ),
        pytest.param(
            LiteralExpression(1, Int64Type()),
            ColumnReference("a", Int64Type()),
            False,
            id="different_expr",
        ),
    ],
)
def test_literals_equal(
    ref1: LiteralExpression, ref2: RelationalExpression, output: bool
):
    assert ref1.equals(ref2) == output


@pytest.mark.parametrize(
    "ordering, output",
    [
        pytest.param(
            ColumnOrdering(ColumnReference("a", Int64Type()), True, True),
            "ColumnOrdering(column=Column(name=a, type=Int64Type()), ascending=True, nulls_first=True)",
            id="asc_nulls_first",
        ),
        pytest.param(
            ColumnOrdering(ColumnReference("b", Int64Type()), True, False),
            "ColumnOrdering(column=Column(name=b, type=Int64Type()), ascending=True, nulls_first=False)",
            id="asc_nulls_last",
        ),
        pytest.param(
            ColumnOrdering(ColumnReference("c", Int64Type()), False, True),
            "ColumnOrdering(column=Column(name=c, type=Int64Type()), ascending=False, nulls_first=True)",
            id="desc_nulls_first",
        ),
        pytest.param(
            ColumnOrdering(ColumnReference("d", Int64Type()), False, False),
            "ColumnOrdering(column=Column(name=d, type=Int64Type()), ascending=False, nulls_first=False)",
            id="desc_nulls_last",
        ),
    ],
)
def test_column_ordering_to_string(ordering: ColumnOrdering, output: str):
    assert ordering.to_string() == output


@pytest.mark.parametrize(
    "ordering1, ordering2, output",
    [
        pytest.param(
            ColumnOrdering(ColumnReference("a", Int64Type()), True, True),
            ColumnOrdering(ColumnReference("a", Int64Type()), True, True),
            True,
            id="same_ordering",
        ),
        pytest.param(
            ColumnOrdering(ColumnReference("a", Int64Type()), True, True),
            ColumnOrdering(ColumnReference("b", Int64Type()), True, True),
            False,
            id="different_column",
        ),
        pytest.param(
            ColumnOrdering(ColumnReference("a", Int64Type()), True, True),
            ColumnOrdering(ColumnReference("a", Int64Type()), False, True),
            False,
            id="different_asc",
        ),
        pytest.param(
            ColumnOrdering(ColumnReference("a", Int64Type()), True, True),
            ColumnOrdering(ColumnReference("a", Int64Type()), True, False),
            False,
            id="different_nulls_first",
        ),
        pytest.param(
            ColumnOrdering(ColumnReference("a", Int64Type()), True, True),
            LiteralExpression(1, Int64Type()),
            False,
            id="different_nodes",
        ),
    ],
)
def test_column_ordering_equal(ordering1: ColumnOrdering, ordering2: Any, output: bool):
    assert (ordering1 == ordering2) == output


@pytest.mark.parametrize(
    "expr, output",
    [
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            "Call(op=Function[LOWER], inputs=[Column(name=a, type=StringType())], return_type=StringType())",
            id="lower",
        ),
        pytest.param(
            CallExpression(SUM, Int64Type(), [ColumnReference("a", Int64Type())]),
            "Call(op=Function[SUM], inputs=[Column(name=a, type=Int64Type())], return_type=Int64Type())",
            id="sum",
        ),
    ],
)
def test_call_expressions_to_string(expr: CallExpression, output: str):
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
            CallExpression(SUM, Int64Type(), [ColumnReference("a", Int64Type())]),
            CallExpression(LOWER, Int64Type(), [ColumnReference("a", Int32Type())]),
            False,
            id="different_arg_type",
        ),
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            CallExpression(SUM, Int64Type(), [ColumnReference("a", StringType())]),
            False,
            id="different_op",
        ),
        pytest.param(
            CallExpression(SUM, Int32Type(), [ColumnReference("a", Int32Type())]),
            CallExpression(SUM, Int64Type(), [ColumnReference("a", Int32Type())]),
            False,
            id="different_return_type",
        ),
        pytest.param(
            CallExpression(LOWER, StringType(), [ColumnReference("a", StringType())]),
            LiteralExpression(1, Int64Type()),
            False,
            id="different_expr",
        ),
    ],
)
def test_call_expressions_equal(
    expr1: CallExpression, expr2: RelationalExpression, output: bool
):
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
            CallExpression(SUM, Int64Type(), [ColumnReference("a", Int64Type())]),
            True,
            id="sum",
        ),
    ],
)
def test_call_expression_is_aggregation(expr: CallExpression, output: bool):
    assert expr.is_aggregation == output
