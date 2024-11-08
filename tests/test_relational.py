"""
TODO: add file-level docstring
"""

import pytest

from pydough.pydough_ast.expressions.literal import Literal
from pydough.pydough_ast.expressions.simple_column_reference import (
    SimpleColumnReference,
)
from pydough.relational import Column
from pydough.relational.scan import Scan
from pydough.types import Int64Type


def make_simple_column_reference(name: str) -> SimpleColumnReference:
    """
    Make a simple column reference with type int64 and
    the given name. This is used for generating various relational nodes.

    Args:
        name (str): The name of the column in the input.

    Returns:
        SimpleColumnReference: The AST node for the column.
    """
    return SimpleColumnReference(name, Int64Type())


def make_column(name: str) -> Column:
    """
    Make an Int64 column with the given name. This is used
    for generating various relational nodes.

    Note: This doesn't handle renaming a column.

    Args:
        name (str): The name of the column in both the input and the
        current node.

    Returns:
        Column: The output column.
    """
    return Column(name, make_simple_column_reference(name))


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
    scan = Scan("table", [make_column("a"), make_column("b")])
    assert scan.inputs == []


@pytest.mark.parametrize(
    "scan_node, output",
    [
        pytest.param(
            Scan("table", [make_column("a"), make_column("b")]),
            "empty",
            id="no_orderings",
        ),
        pytest.param(
            # TODO: Update orderings when ASC/DESC is merged in the AST.
            Scan(
                "table",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            "empty",
            id="with_orderings",
        ),
        pytest.param(
            Scan("table", []),
            "empty",
            id="no_columns",
        ),
    ],
)
def test_scan_to_string(scan_node, output):
    assert scan_node.to_string() == output


# @pytest.mark.parametrize(
#     "first_scan, second_scan, output",
#     [
#         pytest.param(
#             id="different_table",
#         ),
#         pytest.param(
#             id="matching_columns",
#         ),
#         pytest.param(
#             id="disjoint_columns",
#         ),
#         pytest.param(
#             id="overlapping_columns",
#         ),
#         pytest.param(
#             id="matching_orderings",
#         ),
#         pytest.param(
#             id="disjoint_orderings",
#         ),
#         pytest.param(
#             id="overlapping_orderings",
#         ),
#         pytest.param(
#             id="conflicting_orderings",
#         ),
#     ],
# )
# def test_scan_can_merge():
#     pass


# @pytest.mark.parametrize(
#     "first_scan, second_scan, output",
#     [
#         pytest.param(
#             id="matching_columns",
#         ),
#         pytest.param(
#             id="disjoint_columns",
#         ),
#         pytest.param(
#             id="overlapping_columns",
#         ),
#         pytest.param(
#             id="matching_orderings",
#         ),
#     ],
# )
# def test_scan_merge():
#     pass


# @pytest.mark.parametrize(
#     "first_scan, second_scan, output",
#     [
#         pytest.param(
#             id="different_table",
#         ),
#         pytest.param(
#             id="disjoint_orderings",
#         ),
#         pytest.param(
#             id="overlapping_orderings",
#         ),
#         pytest.param(
#             id="conflicting_orderings",
#         ),
#     ],
# )
# def test_scan_invalid_merge():
#     pass
