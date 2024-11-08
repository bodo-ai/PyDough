"""
TODO: add file-level docstring
"""

import pytest

from pydough.pydough_ast.expressions.literal import Literal
from pydough.relational import Column
from pydough.types import Int64Type


def test_column_equal():
    """
    Test that the column definition properly implements equality
    based on its elements.
    """
    column1 = Column("a", Literal(1, Int64Type()))
    column2 = Column("a", Literal(1, Int64Type()))
    column3 = Column("b", Literal(1, Int64Type()))
    column4 = Column("a", Literal(2, Int64Type()))
    assert column1 == column2
    assert column1 != column3
    assert column1 != column4


def test_scan_inputs():
    pass


@pytest.mark.parametrize(
    "scan_node, output",
    [
        pytest.param(
            id="no_orderings",
        ),
        pytest.param(
            id="with_orderings",
        ),
        pytest.param(
            id="no_columns",
        ),
    ],
)
def test_scan_to_string():
    pass


@pytest.mark.parametrize(
    "first_scan, second_scan, output",
    [
        pytest.param(
            id="different_table",
        ),
        pytest.param(
            id="disjoint_columns",
        ),
        pytest.param(
            id="overlapping_columns",
        ),
        pytest.param(
            id="disjoint_orderings",
        ),
        pytest.param(
            id="overlapping_orderings",
        ),
        pytest.param(
            id="conflicting_orderings",
        ),
    ],
)
def test_scan_can_merge():
    pass


@pytest.mark.parametrize(
    "first_scan, second_scan, output",
    [
        pytest.param(
            id="disjoint_columns",
        ),
        pytest.param(
            id="overlapping_columns",
        ),
        pytest.param(
            id="disjoint_orderings",
        ),
        pytest.param(
            id="overlapping_orderings",
        ),
        pytest.param(
            id="conflicting_orderings",
        ),
    ],
)
def test_scan_merge():
    pass


@pytest.mark.parametrize(
    "first_scan, second_scan, output",
    [
        pytest.param(
            id="different_table",
        ),
    ],
)
def test_scan_invalid_merge():
    pass
