"""
TODO: add file-level docstring

General TODO for all tests:
Update orderings when ASC/DESC is merged in the AST.

General TODO for all aggregate tests:
Update once we have AST nodes for aggregate functions.
"""

import pytest

from pydough.pydough_ast.expressions.literal import Literal
from pydough.pydough_ast.expressions.simple_column_reference import (
    SimpleColumnReference,
)
from pydough.relational import Column, Relational
from pydough.relational.aggregate import Aggregate
from pydough.relational.limit import Limit
from pydough.relational.project import Project
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


def make_literal_column(name: str, value: int) -> Column:
    """
    Make a literal Int64 column with the given name. This is used
    for generating various relational nodes.

    Args:
        name (str): The name of the column.
        value (int): The value of the literal.

    Returns:
        Column: The output column.
    """
    return Column(name, Literal(value, Int64Type()))


def make_limit_literal(limit: int) -> Literal:
    """
    Make a literal Int64 with the given limit. This is used for
    generating various relational nodes.

    Args:
        limit (int): The value of the literal.

    Returns:
        Literal: The output literal.
    """
    return Literal(limit, Int64Type())


def test_column_equal():
    """
    Test that the column definition properly implements equality
    based on its elements.
    """
    column1 = make_literal_column("a", 1)
    column2 = make_literal_column("a", 1)
    column3 = make_literal_column("b", 1)
    column4 = make_literal_column("a", 2)
    assert column1 == column2
    assert column1 != column3
    assert column1 != column4


def build_simple_scan() -> Scan:
    # Helper function to generate a simple scan node for when
    # relational operators need an input.
    return Scan("table", [make_column("a"), make_column("b")])


def test_scan_inputs():
    scan = build_simple_scan()
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
        pytest.param(
            Scan("table1", [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            False,
            id="different_nodes",
        ),
    ],
)
def test_scan_equals(first_scan: Scan, second_scan: Relational, output: bool):
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


@pytest.mark.parametrize(
    "project, output",
    [
        pytest.param(
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            "PROJECT(columns=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], orderings=[])",
            id="no_orderings",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            "PROJECT(columns=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], orderings=[Column(a)])",
            id="with_orderings",
        ),
        pytest.param(
            Project(build_simple_scan(), []),
            "PROJECT(columns=[], orderings=[])",
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
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            True,
            id="matching_projects_no_orderings",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            True,
            id="matching_projects_with_orderings",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            False,
            id="different_orderings",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("a")]),
            False,
            id="different_columns",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a")]),
            Project(build_simple_scan(), [make_literal_column("a", 1)]),
            False,
            id="conflicting_column_definitions",
        ),
        pytest.param(
            Project(build_simple_scan(), []),
            Project(Scan("table2", [], []), []),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Scan("table1", [make_column("a"), make_column("b")]),
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
    "first_project, second_project, output",
    [
        pytest.param(
            Project(build_simple_scan(), []),
            Project(Scan("table2", [], []), []),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            True,
            id="matching_columns",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("c"), make_column("d")]),
            True,
            id="disjoint_columns",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("b"), make_column("c")]),
            True,
            id="overlapping_columns",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a")]),
            Project(build_simple_scan(), [make_literal_column("a", 1)]),
            False,
            id="conflict_column_definitions",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            # Note: Eventually this should be legal
            False,
            id="matching_orderings",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            False,
            id="disjoint_orderings",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Project(
                build_simple_scan(),
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
def test_project_can_merge(
    first_project: Project, second_project: Project, output: bool
):
    assert first_project.can_merge(second_project) == output


@pytest.mark.parametrize(
    "first_project, second_project, output",
    [
        pytest.param(
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            id="matching_columns",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("c"), make_column("d")]),
            Project(
                build_simple_scan(),
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
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            Project(build_simple_scan(), [make_column("b"), make_column("c")]),
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b"), make_column("c")],
            ),
            id="overlapping_columns",
        ),
    ],
)
def test_project_merge(
    first_project: Project, second_project: Project, output: Project
):
    assert first_project.merge(second_project) == output


@pytest.mark.parametrize(
    "first_project, second_project",
    [
        pytest.param(
            Project(build_simple_scan(), []),
            Project(Scan("table2", [], []), []),
            id="unequal_inputs",
        ),
        pytest.param(
            Project(build_simple_scan(), [make_column("a")]),
            Project(build_simple_scan(), [make_literal_column("a", 1)]),
            id="conflict_column_definitions",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            # Note: Eventually this should be legal
            id="matching_orderings",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            id="disjoint_orderings",
        ),
        pytest.param(
            Project(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Project(
                build_simple_scan(),
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
def test_project_invalid_merge(first_project: Project, second_project: Project):
    with pytest.raises(ValueError, match="Cannot merge nodes"):
        first_project.merge(second_project)


@pytest.mark.parametrize(
    "limit, output",
    [
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(1),
                [make_column("a"), make_column("b")],
            ),
            "LIMIT(limit=1, columns=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], orderings=[])",
            id="no_orderings_limit_1",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
            ),
            "LIMIT(limit=5, columns=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], orderings=[])",
            id="no_orderings_limit_5",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            "LIMIT(limit=10, columns=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], orderings=[Column(a)])",
            id="with_orderings",
        ),
        pytest.param(
            Limit(build_simple_scan(), make_limit_literal(10), []),
            "LIMIT(limit=10, columns=[], orderings=[])",
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
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            True,
            id="matching_limits_no_orderings",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
            ),
            False,
            id="different_limits_no_orderings",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            True,
            id="matching_limits_with_orderings",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            False,
            id="different_limits_with_orderings",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            False,
            id="different_orderings",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(build_simple_scan(), make_limit_literal(10), [make_column("a")]),
            False,
            id="different_columns",
        ),
        pytest.param(
            Limit(build_simple_scan(), make_limit_literal(5), []),
            Limit(Scan("table2", [], []), make_limit_literal(5), []),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Project(build_simple_scan(), [make_column("a"), make_column("b")]),
            False,
            id="different_nodes",
        ),
    ],
)
def test_limit_equals(first_limit: Limit, second_limit: Relational, output: bool):
    assert first_limit.equals(second_limit) == output


@pytest.mark.parametrize(
    "first_limit, second_limit, output",
    [
        pytest.param(
            Limit(build_simple_scan(), make_limit_literal(10), []),
            Limit(Scan("table2", [], []), make_limit_literal(10), []),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            True,
            id="matching_columns_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
            ),
            False,
            id="matching_columns_unequal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("c"), make_column("d")],
            ),
            True,
            id="disjoint_columns_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("c"), make_column("d")],
            ),
            False,
            id="disjoint_columns_unequal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("b"), make_column("c")],
            ),
            True,
            id="overlapping_columns_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("b"), make_column("c")],
            ),
            False,
            id="overlapping_columns_unequal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            # Note: Eventually this should be legal
            False,
            id="matching_orderings_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            # Note: Eventually this should be legal
            False,
            id="matching_orderings_unequal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            False,
            id="disjoint_orderings",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            # Note: If we allow merging orderings this should become legal.
            False,
            id="overlapping_orderings_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            # Note: If we allow merging orderings this should become legal.
            False,
            id="overlapping_orderings_unequal_limits",
        ),
        # TODO: Add a conflicting ordering test where A is ASC vs DESC.
        # Depends on other code changes merging.
    ],
)
def test_limit_can_merge(first_limit: Limit, second_limit: Limit, output: bool):
    assert first_limit.can_merge(second_limit) == output


@pytest.mark.parametrize(
    "first_limit, second_limit, output",
    [
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            id="matching_columns_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("c"), make_column("d")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [
                    make_column("a"),
                    make_column("b"),
                    make_column("c"),
                    make_column("d"),
                ],
            ),
            id="disjoint_columns_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("b"), make_column("c")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b"), make_column("c")],
            ),
            id="overlapping_columns_equal_limits",
        ),
    ],
)
def test_limit_merge(first_limit: Limit, second_limit: Limit, output: Limit):
    assert first_limit.merge(second_limit) == output


@pytest.mark.parametrize(
    "first_limit, second_limit",
    [
        pytest.param(
            Limit(build_simple_scan(), make_limit_literal(10), []),
            Limit(Scan("table2", [], []), make_limit_literal(10), []),
            id="unequal_inputs",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
            ),
            id="matching_columns_unequal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("c"), make_column("d")],
            ),
            id="disjoint_columns_unequal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("b"), make_column("c")],
            ),
            id="overlapping_columns_unequal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            # Note: Eventually this should be legal
            id="matching_orderings_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            # Note: Eventually this should be legal
            id="matching_orderings_unequal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("b")],
            ),
            id="disjoint_orderings",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            # Note: If we allow merging orderings this should become legal.
            id="overlapping_orderings_equal_limits",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_limit_literal(10),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a")],
            ),
            Limit(
                build_simple_scan(),
                make_limit_literal(5),
                [make_column("a"), make_column("b")],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            id="overlapping_orderings_unequal_limits",
        ),
        # TODO: Add a conflicting ordering test where A is ASC vs DESC.
        # Depends on other code changes merging.
    ],
)
def test_limit_invalid_merge(first_limit: Limit, second_limit: Limit):
    with pytest.raises(ValueError, match="Cannot merge nodes"):
        first_limit.merge(second_limit)


@pytest.mark.parametrize(
    "agg, output",
    [
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            "AGGREGATE(keys=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], aggregations=[], orderings=[])",
            id="no_orderings_no_aggregates",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            "AGGREGATE(keys=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], aggregations=[], orderings=[])",
            id="no_orderings_no_keys_no_aggregates",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            "AGGREGATE(keys=[Column(name='a', expr=Column(a)), Column(name='b', expr=Column(b))], aggregations=[], orderings=[Column(a)])",
            id="with_orderings_no_aggregates",
        ),
    ],
)
def test_aggregate_to_string(agg: Aggregate, output: str):
    assert agg.to_string() == output


@pytest.mark.parametrize(
    "first_agg, second_agg, output",
    [
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            True,
            id="same_keys_no_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("c"), make_column("d")],
                [],
            ),
            False,
            id="different_keys_no_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a")],
                [],
            ),
            False,
            id="subset_keys_no_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            True,
            id="same_keys_with_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("b")],
            ),
            False,
            id="same_keys_different_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("c"), make_column("d")],
                [],
                [make_simple_column_reference("a")],
            ),
            False,
            id="different_keys_with_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a")],
                [],
                [make_simple_column_reference("a")],
            ),
            False,
            id="subset_keys_with_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                Scan("table2", [make_column("a"), make_column("b")], []),
                [make_column("a"), make_column("b")],
                [],
            ),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Scan("table2", [make_column("a"), make_column("b")], []),
            False,
            id="different_nodes",
        ),
    ],
)
def test_aggregate_equals(first_agg: Aggregate, second_agg: Relational, output: bool):
    assert first_agg.equals(second_agg) == output


@pytest.mark.parametrize(
    "first_agg, second_agg, output",
    [
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                Scan("table2", [], []),
                [make_column("a"), make_column("b")],
                [],
            ),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            True,
            id="matching_keys_no_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("c"), make_column("d")],
                [],
            ),
            False,
            id="disjoint_keys_no_ordering",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("b")],
                [],
            ),
            False,
            id="subset_keys_no_ordering",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("b"), make_column("c")],
                [],
            ),
            False,
            id="overlapping_keys_no_ordering",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            False,
            id="matching_ordering_matching_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a")],
                [],
                [make_simple_column_reference("a")],
            ),
            False,
            id="matching_ordering_overlapping_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("b")],
            ),
            False,
            id="disjoint_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            False,
            # Note: If we allow merging orderings this should become legal.
            id="overlapping_orderings_equal_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            False,
            id="overlapping_orderings_overlapping_keys",
        ),
        # TODO: Add a conflicting ordering test where A is ASC vs DESC.
        # Depends on other code changes merging
    ],
)
def test_aggregate_can_merge(first_agg: Aggregate, second_agg: Aggregate, output: bool):
    assert first_agg.can_merge(second_agg) == output


@pytest.mark.parametrize(
    "first_agg, second_agg, output",
    [
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            id="matching_keys_no_orderings",
        ),
    ],
)
def test_aggregate_merge(
    first_agg: Aggregate, second_agg: Aggregate, output: Aggregate
):
    assert first_agg.merge(second_agg) == output


@pytest.mark.parametrize(
    "first_agg, second_agg",
    [
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                Scan("table2", [], []),
                [make_column("a"), make_column("b")],
                [],
            ),
            id="unequal_inputs",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("c"), make_column("d")],
                [],
            ),
            id="disjoint_keys_no_ordering",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("b")],
                [],
            ),
            id="subset_keys_no_ordering",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("b"), make_column("c")],
                [],
            ),
            id="overlapping_keys_no_ordering",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            id="matching_ordering_matching_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a")],
                [],
                [make_simple_column_reference("a")],
            ),
            id="matching_ordering_overlapping_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("b")],
            ),
            id="disjoint_orderings",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            # Note: If we allow merging orderings this should become legal.
            id="overlapping_orderings_equal_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                [make_column("a")],
                [],
                [make_simple_column_reference("a")],
            ),
            Aggregate(
                build_simple_scan(),
                [make_column("a"), make_column("b")],
                [],
                [make_simple_column_reference("a"), make_simple_column_reference("b")],
            ),
            id="overlapping_orderings_overlapping_keys",
        ),
        # TODO: Add a conflicting ordering test where A is ASC vs DESC.
        # Depends on other code changes merging
    ],
)
def test_aggregate_invalid_merge(first_agg: Aggregate, second_agg: Aggregate):
    with pytest.raises(ValueError, match="Cannot merge nodes"):
        first_agg.merge(second_agg)
