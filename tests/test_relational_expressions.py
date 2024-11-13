"""
Tests the defined behavior of the expressions
used in relational nodes. These are meant to represent
base expressions like column accesses, literals, and
functions.
"""

import pytest

from pydough.relational.relational_expressions import RelationalExpression
from pydough.relational.relational_expressions.column_reference import ColumnReference
from pydough.types import Int64Type, StringType


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
    ],
    # TODO: Add a test for different types when we add literals
)
def test_column_reference_equals(
    ref1: ColumnReference, ref2: RelationalExpression, output: bool
):
    assert ref1.equals(ref2) == output
