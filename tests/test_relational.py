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
            Scan("table1", [make_column("a"), make_column("b")]),
            "SCAN(table=table1, columns=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], orderings=[])",
            id="no_orderings",
        ),
        pytest.param(
            # TODO: Update orderings when ASC/DESC is merged in the AST.
            Scan(
                "table2",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            "SCAN(table=table2, columns=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], orderings=[Column(a)])",
            id="with_orderings",
        ),
        pytest.param(
            Scan("table3", []),
            "SCAN(table=table3, columns=[], orderings=[])",
            id="no_columns",
        ),
    ],
)
def test_scan_to_string(scan_node: Scan, output: str):
    assert scan_node.to_string() == output


@pytest.mark.parametrize(
    "first_scan, second_scan, output",
    [
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("a"), make_column("b")]),
            True,
            id="matching_scans_no_orderings",
        ),
        pytest.param(
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            True,
            id="matching_scans_with_orderings",
        ),
        pytest.param(
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            False,
            id="different_orderings",
        ),
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("a")]),
            False,
            id="different_columns",
        ),
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table2", [make_column("a"), make_column("b")]),
            False,
            id="different_tables",
        ),
    ],
)
def test_scan_equals(first_scan: Scan, second_scan: Scan, output: bool):
    assert first_scan.equals(second_scan) == output


@pytest.mark.parametrize(
    "first_scan, second_scan, output",
    [
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table2", [make_column("a"), make_column("b")]),
            False,
            id="different_table",
        ),
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("a"), make_column("b")]),
            True,
            id="matching_columns",
        ),
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("c"), make_column("d")]),
            True,
            id="disjoint_columns",
        ),
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("b"), make_column("c")]),
            True,
            id="overlapping_columns",
        ),
        pytest.param(
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            # Note: Eventually this should be legal
            False,
            id="matching_orderings",
        ),
        pytest.param(
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            False,
            id="disjoint_orderings",
        ),
        pytest.param(
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            # Note: If we allow merging orderings this should become legal.
            False,
            id="overlapping_orderings",
        ),
        # TODO: Add a conflicting ordering test where A is ASC vs DESC.
        # Depends on other code changes merging.
    ],
)
def test_scan_can_merge(first_scan: Scan, second_scan: Scan, output: bool):
    assert first_scan.can_merge(second_scan) == output


@pytest.mark.parametrize(
    "first_scan, second_scan, output",
    [
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("a"), make_column("b")]),
            id="matching_columns",
        ),
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("c"), make_column("d")]),
            Scan(
                "table1",
                [
                    make_column("a"),
                    make_column("b"),
                    make_column("c"),
                    make_column("d"),
                ],
            ),
            id="disjoint_columns",
        ),
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("b"), make_column("c")]),
            Scan("table1", [make_column("a"), make_column("b"), make_column("c")]),
            id="overlapping_columns",
        ),
    ],
)
def test_scan_merge(first_scan: Scan, second_scan: Scan, output: Scan):
    assert first_scan.merge(second_scan) == output


@pytest.mark.parametrize(
    "first_scan, second_scan",
    [
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Scan("table2", [make_column("a"), make_column("b")]),
            id="different_table",
        ),
        pytest.param(
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            # Note: Eventually this should be legal
            id="matching_orderings",
        ),
        pytest.param(
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            id="disjoint_orderings",
        ),
        pytest.param(
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Scan(
                "table1",
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            # Note: If we allow merging orderings this should become legal.
            id="overlapping_orderings",
        ),
        # TODO: Add a conflicting ordering test where A is ASC vs DESC.
        # Depends on other code changes merging.
    ],
)
def test_scan_invalid_merge(first_scan: Scan, second_scan: Scan):
    with pytest.raises(ValueError, match="Cannot merge nodes"):
        first_scan.merge(second_scan)
