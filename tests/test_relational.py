"""
TODO: add file-level docstring
"""

from typing import Any

import pytest

from pydough.relational import Relational
from pydough.relational.limit import Limit
from pydough.relational.project import Project
from pydough.relational.relational_expressions.column_reference import ColumnReference
from pydough.relational.relational_expressions.literal_expression import (
    LiteralExpression,
)
from pydough.relational.scan import Scan
from pydough.types import Int64Type, PyDoughType, UnknownType


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


def make_relational_literal(value: Any, typ: PyDoughType | None = None):
    """
    Make a literal given value and type. This is used for
    generating various relational nodes.

    Args:
        value (Any): The value of the literal.

    Returns:
        Literal: The output literal.
    """
    pydough_type = typ if typ is not None else UnknownType()
    return LiteralExpression(value, pydough_type)


def build_simple_scan() -> Scan:
    # Helper function to generate a simple scan node for when
    # relational operators need an input.
    return Scan(
        "table",
        {
            "a": make_relational_column_reference("a"),
            "b": make_relational_column_reference("b"),
        },
    )


def test_scan_inputs():
    scan = build_simple_scan()
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
            "SCAN(table=table1, columns={'a': Column(name=a, type=UnknownType()), 'b': Column(name=b, type=UnknownType())})",
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
        pytest.param(
            Scan(
                "table1",
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Project(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            False,
            id="different_nodes",
        ),
    ],
)
def test_scan_equals(first_scan: Scan, second_scan: Relational, output: bool):
    assert first_scan.equals(second_scan) == output


@pytest.mark.parametrize(
    "project, output",
    [
        pytest.param(
            Project(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            "PROJECT(columns={'a': Column(name=a, type=UnknownType()), 'b': Column(name=b, type=UnknownType())})",
            id="no_orderings",
        ),
        pytest.param(
            Project(build_simple_scan(), {}),
            "PROJECT(columns={})",
            id="no_columns",
        ),
    ],
)
def test_project_to_string(project: Project, output: str):
    assert project.to_string() == output


@pytest.mark.parametrize(
    "first_project, second_project, output",
    [
        pytest.param(
            Project(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Project(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            True,
            id="matching_projects",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Project(build_simple_scan(), {"a": make_relational_column_reference("a")}),
            False,
            id="different_columns",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Project(
                build_simple_scan(),
                {
                    "b": make_relational_column_reference("b"),
                    "a": make_relational_column_reference("a"),
                },
            ),
            True,
            id="reordered_columns",
        ),
        pytest.param(
            Project(build_simple_scan(), {"a": make_relational_column_reference("a")}),
            Project(build_simple_scan(), {"a": make_relational_column_reference("b")}),
            False,
            id="conflicting_column_mappings",
        ),
        pytest.param(
            Project(build_simple_scan(), {"a": make_relational_column_reference("a")}),
            Project(build_simple_scan(), {"a": make_relational_literal(1)}),
            False,
            id="conflicting_column_values",
        ),
        pytest.param(
            Project(build_simple_scan(), {}),
            Project(Scan("table2", {}), {}),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                {
                    "b": make_relational_column_reference("b"),
                    "a": make_relational_column_reference("a"),
                },
            ),
            Scan(
                "table1",
                {
                    "b": make_relational_column_reference("b"),
                    "a": make_relational_column_reference("a"),
                },
            ),
            False,
            id="different_nodes",
        ),
    ],
)
def test_project_equals(
    first_project: Project, second_project: Relational, output: bool
):
    assert first_project.equals(second_project) == output


@pytest.mark.parametrize(
    "limit, output",
    [
        pytest.param(
            Limit(
                build_simple_scan(),
                make_relational_literal(1, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            "LIMIT(limit=Literal(value=1, type=Int64Type()), columns={'a': Column(name=a, type=UnknownType()), 'b': Column(name=b, type=UnknownType())}, orderings=[])",
            id="limit_1",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_relational_literal(5, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            "LIMIT(limit=Literal(value=5, type=Int64Type()), columns={'a': Column(name=a, type=UnknownType()), 'b': Column(name=b, type=UnknownType())}, orderings=[])",
            id="limit_5",
        ),
        pytest.param(
            Limit(build_simple_scan(), make_relational_literal(10, Int64Type()), {}),
            "LIMIT(limit=Literal(value=10, type=Int64Type()), columns={}, orderings=[])",
            id="no_columns",
        ),
    ],
)
def test_limit_to_string(limit: Limit, output: str):
    assert limit.to_string() == output


@pytest.mark.parametrize(
    "first_limit, second_limit, output",
    [
        pytest.param(
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            True,
            id="matching_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Limit(
                build_simple_scan(),
                make_relational_literal(5, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            False,
            id="different_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                },
            ),
            False,
            id="different_columns",
        ),
        pytest.param(
            Limit(build_simple_scan(), make_relational_literal(5, Int64Type()), {}),
            Limit(Scan("table2", {}), make_relational_literal(5, Int64Type()), {}),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Project(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            False,
            id="different_nodes",
        ),
    ],
)
def test_limit_equals(first_limit: Limit, second_limit: Relational, output: bool):
    assert first_limit.equals(second_limit) == output
