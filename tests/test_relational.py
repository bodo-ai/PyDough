"""
TODO: add file-level docstring
"""

import pytest

from pydough.relational import RelationalColumn
from pydough.relational.relational_expressions.column_reference import ColumnReference
from pydough.relational.scan import Scan
from pydough.types import Int64Type, PyDoughType, StringType, UnknownType


def make_relational_column(
    name: str, typ: PyDoughType | None = None
) -> RelationalColumn:
    """
    Make a relational column with the given name and type. This is used
    for generating various relational nodes.

    Note: This doesn't handle renaming a column.

    Args:
        name (str): The name of the column in both the input and the
        current node.

    Returns:
        Column: The output column.
    """
    pydough_type = typ if typ is not None else UnknownType()
    return RelationalColumn(name, ColumnReference(name, pydough_type))


def test_relation_column_equals():
    """
    Test that the relational column definition properly implements equality
    based on its elements.
    """
    column1 = RelationalColumn("a", ColumnReference("a", Int64Type()))
    column2 = RelationalColumn("a", ColumnReference("a", Int64Type()))
    column3 = RelationalColumn("b", ColumnReference("a", StringType()))
    column4 = RelationalColumn("a", ColumnReference("b", Int64Type()))
    assert column1 == column2
    assert column1 != column3
    assert column1 != column4


def test_scan_inputs():
    scan = Scan("table", [make_relational_column("a"), make_relational_column("b")])
    assert scan.inputs == []


@pytest.mark.parametrize(
    "scan_node, output",
    [
        pytest.param(
            Scan("table1", [make_relational_column("a"), make_relational_column("b")]),
            "SCAN(table=table1, columns=[RelationalColumn(name='a', expr=Column(a)), RelationalColumn(name='b', expr=Column(b))])",
            id="base_column",
        ),
        pytest.param(
            Scan("table3", []),
            "SCAN(table=table3, columns=[])",
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
            Scan("table1", [make_relational_column("a"), make_relational_column("b")]),
            Scan("table1", [make_relational_column("a"), make_relational_column("b")]),
            True,
            id="matching_scans",
        ),
        pytest.param(
            Scan("table1", [make_relational_column("a"), make_relational_column("b")]),
            Scan("table1", [make_relational_column("a")]),
            False,
            id="different_columns",
        ),
        pytest.param(
            Scan("table1", [make_relational_column("a"), make_relational_column("b")]),
            Scan("table2", [make_relational_column("a"), make_relational_column("b")]),
            False,
            id="different_tables",
        ),
    ],
)
def test_scan_equals(first_scan: Scan, second_scan: Scan, output: bool):
    assert first_scan.equals(second_scan) == output
