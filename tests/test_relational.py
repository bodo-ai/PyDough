"""
TODO: add file-level docstring
"""

import pytest

from pydough.relational.relational_expressions.column_reference import ColumnReference
from pydough.relational.scan import Scan
from pydough.types import PyDoughType, UnknownType


def make_relational_column_reference(
    name: str, typ: PyDoughType | None = None
) -> ColumnReference:
    """
    Make a column reference given name and type. This is used
    for generating various relational nodes.

    Args:
        name (str): The name of the column in the input.

    Returns:
        Column: The output column.
    """
    pydough_type = typ if typ is not None else UnknownType()
    return ColumnReference(name, pydough_type)


def test_scan_inputs():
    scan = Scan(
        "table",
        {
            "a": make_relational_column_reference("a"),
            "b": make_relational_column_reference("b"),
        },
    )
    assert scan.inputs == []


@pytest.mark.parametrize(
    "scan_node, output",
    [
        pytest.param(
            Scan(
                "table1",
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            "SCAN(table=table1, columns={'a': Column(a), 'b': Column(b)})",
            id="base_column",
        ),
        pytest.param(
            Scan("table3", {}),
            "SCAN(table=table3, columns={})",
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
            Scan(
                "table1",
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Scan(
                "table1",
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            True,
            id="matching_scans",
        ),
        pytest.param(
            Scan(
                "table1",
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Scan("table1", {"a": make_relational_column_reference("a")}),
            False,
            id="different_columns",
        ),
        pytest.param(
            Scan(
                "table1",
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Scan(
                "table2",
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            False,
            id="different_tables",
        ),
    ],
)
def test_scan_equals(first_scan: Scan, second_scan: Scan, output: bool):
    assert first_scan.equals(second_scan) == output
