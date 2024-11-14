"""
TODO: add file-level docstring
"""

from typing import Any

import pytest

from pydough.pydough_ast.pydough_operators.expression_operators import EQU, LOWER, SUM
from pydough.relational import Relational
from pydough.relational.aggregate import Aggregate
from pydough.relational.filter import Filter
from pydough.relational.join import Join, JoinType
from pydough.relational.limit import Limit
from pydough.relational.project import Project
from pydough.relational.relational_expressions import (
    ColumnOrdering,
)
from pydough.relational.relational_expressions.call_expression import CallExpression
from pydough.relational.relational_expressions.column_reference import ColumnReference
from pydough.relational.relational_expressions.literal_expression import (
    LiteralExpression,
)
from pydough.relational.root import RelationalRoot
from pydough.relational.scan import Scan
from pydough.types import BooleanType, Int64Type, PyDoughType, StringType, UnknownType


def make_relational_column_reference(
    name: str, typ: PyDoughType | None = None, input_name: str | None = None
) -> ColumnReference:
    """
    Make a column reference given name and type. This is used
    for generating various relational nodes.

    Args:
        name (str): The name of the column in the input.
        typ (PyDoughType | None): The PyDoughType of the column. Defaults to
            None.
        input_name (str | None): The name of the input node. This is
            used by Join to differentiate between the left and right.
            Defaults to None.

    Returns:
        Column: The output column.
    """
    pydough_type = typ if typ is not None else UnknownType()
    return ColumnReference(name, pydough_type, input_name)


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


def make_relational_column_ordering(
    column: ColumnReference, ascending: bool = True, nulls_first: bool = True
):
    """
    Create a column ordering as a function of a Relational column reference
    with the given ascending and nulls_first parameters.

    Args:
        name (str): _description_
        typ (PyDoughType | None, optional): _description_. Defaults to None.
        ascending (bool, optional): _description_. Defaults to True.
        nulls_first (bool, optional): _description_. Defaults to True.

    Returns:
        _type_: _description_
    """
    return ColumnOrdering(column, ascending, nulls_first)


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
        pytest.param(
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a")
                    ),
                    make_relational_column_ordering(
                        make_relational_column_reference("b"), ascending=False
                    ),
                ],
            ),
            "LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'a': Column(name=a, type=UnknownType()), 'b': Column(name=b, type=UnknownType())}, orderings=[ColumnOrdering(column=Column(name=a, type=UnknownType()), ascending=True, nulls_first=True), ColumnOrdering(column=Column(name=b, type=UnknownType()), ascending=False, nulls_first=True)])",
            id="orderings",
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
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a")
                    )
                ],
            ),
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a")
                    )
                ],
            ),
            True,
            id="matching_ordering",
        ),
        pytest.param(
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a"), ascending=True
                    )
                ],
            ),
            Limit(
                build_simple_scan(),
                make_relational_literal(10, Int64Type()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a"), ascending=False
                    )
                ],
            ),
            False,
            id="different_orderings",
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


@pytest.mark.parametrize(
    "literal",
    [
        pytest.param(make_relational_literal(1), id="unknown_type"),
        pytest.param(make_relational_literal(1, StringType()), id="string_type"),
    ],
)
def test_invalid_limit(literal: LiteralExpression):
    """
    Test to verify that we raise an error when the limit is not an integer
    type regardless of the value type.
    """
    with pytest.raises(AssertionError, match="Limit must be an integer type"):
        Limit(build_simple_scan(), literal, {})


@pytest.mark.parametrize(
    "agg, output",
    [
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                },
                {
                    "b": CallExpression(
                        SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                    )
                },
            ),
            "AGGREGATE(keys={'a': Column(name=a, type=UnknownType())}, aggregations={'b': Call(op=Function[SUM], inputs=[Column(name=b, type=Int64Type())], return_type=Int64Type())})",
            id="key_and_agg",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                {},
            ),
            "AGGREGATE(keys={'a': Column(name=a, type=UnknownType()), 'b': Column(name=b, type=UnknownType())}, aggregations={})",
            id="no_aggregates",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {},
                {
                    "a": CallExpression(
                        SUM, Int64Type(), [ColumnReference("a", Int64Type())]
                    ),
                    "b": CallExpression(
                        SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                    ),
                },
            ),
            "AGGREGATE(keys={}, aggregations={'a': Call(op=Function[SUM], inputs=[Column(name=a, type=Int64Type())], return_type=Int64Type()), 'b': Call(op=Function[SUM], inputs=[Column(name=b, type=Int64Type())], return_type=Int64Type())})",
            id="no_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {},
                {},
            ),
            "AGGREGATE(keys={}, aggregations={})",
            id="no_keys_no_aggregates",
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
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                {},
            ),
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                {},
            ),
            True,
            id="same_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {},
                {
                    "a": CallExpression(
                        SUM, Int64Type(), [ColumnReference("a", Int64Type())]
                    ),
                    "b": CallExpression(
                        SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                    ),
                },
            ),
            Aggregate(
                build_simple_scan(),
                {},
                {
                    "a": CallExpression(
                        SUM, Int64Type(), [ColumnReference("a", Int64Type())]
                    ),
                    "b": CallExpression(
                        SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                    ),
                },
            ),
            True,
            id="same_aggs",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                },
                {
                    "b": CallExpression(
                        SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                    )
                },
            ),
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                },
                {
                    "b": CallExpression(
                        SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                    )
                },
            ),
            True,
            id="same_agg_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                {},
            ),
            Aggregate(
                build_simple_scan(),
                {
                    "c": make_relational_column_reference("c"),
                    "d": make_relational_column_reference("d"),
                },
                {},
            ),
            False,
            id="different_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                },
                {
                    "b": CallExpression(
                        SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                    )
                },
            ),
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                },
                {
                    "c": CallExpression(
                        SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                    )
                },
            ),
            False,
            id="different_agg",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                {},
            ),
            Aggregate(
                build_simple_scan(),
                {"a": make_relational_column_reference("a")},
                {},
            ),
            False,
            id="subset_keys",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                {},
            ),
            Aggregate(
                Scan(
                    "table2",
                    {
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                {},
            ),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Aggregate(
                build_simple_scan(),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                {},
            ),
            Scan(
                "table2",
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
def test_aggregate_equals(first_agg: Aggregate, second_agg: Relational, output: bool):
    assert first_agg.equals(second_agg) == output


def test_aggregate_requires_aggregations():
    """
    Test to verify that we raise an error when the aggregate node is
    created without non-aggregation functions.
    """
    with pytest.raises(
        AssertionError,
        match="All functions used in aggregations must be aggregation functions",
    ):
        Aggregate(
            build_simple_scan(),
            {
                "a": make_relational_column_reference("a"),
            },
            {
                "b": CallExpression(
                    LOWER, StringType(), [ColumnReference("b", StringType())]
                )
            },
        )


def test_aggregate_unique_keys():
    """
    Test to verify that we raise an error when the aggregate node has duplicate
    names between keys and aggregations.
    """
    with pytest.raises(
        AssertionError, match="Keys and aggregations must have unique names"
    ):
        Aggregate(
            build_simple_scan(),
            {
                "a": make_relational_column_reference("a"),
            },
            {
                "a": CallExpression(
                    SUM, Int64Type(), [ColumnReference("b", Int64Type())]
                )
            },
        )


@pytest.mark.parametrize(
    "filter, output",
    [
        pytest.param(
            Filter(
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            "FILTER(condition=Literal(value=True, type=BooleanType()), columns={'a': Column(name=a, type=UnknownType()), 'b': Column(name=b, type=UnknownType())})",
            id="true_filter",
        ),
        pytest.param(
            Filter(
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a"),
                        LiteralExpression(1, Int64Type()),
                    ],
                ),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            "FILTER(condition=Call(op=BinaryOperator[==], inputs=[Column(name=a, type=UnknownType()), Literal(value=1, type=Int64Type())], return_type=BooleanType()), columns={'a': Column(name=a, type=UnknownType()), 'b': Column(name=b, type=UnknownType())})",
            id="function_filter",
        ),
    ],
)
def test_filter_to_string(filter: Filter, output: str):
    assert filter.to_string() == output


@pytest.mark.parametrize(
    "first_filter, second_filter, output",
    [
        pytest.param(
            Filter(
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Filter(
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            True,
            id="matching",
        ),
        pytest.param(
            Filter(
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Filter(
                build_simple_scan(),
                LiteralExpression(False, BooleanType()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            False,
            id="different_conds",
        ),
        pytest.param(
            Filter(
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Filter(
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
                {
                    "c": make_relational_column_reference("a"),
                    "d": make_relational_column_reference("b"),
                },
            ),
            False,
            id="different_columns",
        ),
        pytest.param(
            Filter(
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            Filter(
                Scan(
                    "table2",
                    {
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                LiteralExpression(True, BooleanType()),
                {
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            False,
            id="unequal_inputs",
        ),
        pytest.param(
            Filter(
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
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
            id="different_nodes",
        ),
    ],
)
def test_filter_equals(first_filter: Filter, second_filter: Relational, output: bool):
    assert first_filter.equals(second_filter) == output


def test_filter_requires_boolean_condition():
    """
    Test to verify that we raise an error when the filter node is
    created with a non-boolean condition.
    """
    with pytest.raises(AssertionError, match="Filter condition must be a boolean type"):
        Filter(
            build_simple_scan(),
            make_relational_literal(1, Int64Type()),
            {
                "a": make_relational_column_reference("a"),
            },
        )


@pytest.mark.parametrize(
    "root, output",
    [
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            "ROOT(columns=[('a', Column(name=a, type=UnknownType())), ('b', Column(name=b, type=UnknownType()))], orderings=[])",
            id="no_orderings",
        ),
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                make_relational_column_ordering(
                    make_relational_column_reference("a"), ascending=True
                ),
            ),
            "ROOT(columns=[('a', Column(name=a, type=UnknownType())), ('b', Column(name=b, type=UnknownType()))], orderings=ColumnOrdering(column=Column(name=a, type=UnknownType()), ascending=True, nulls_first=True))",
            id="with_orderings",
        ),
    ],
)
def test_root_to_string(root: RelationalRoot, output: str):
    assert root.to_string() == output


@pytest.mark.parametrize(
    "first_root, second_root, output",
    [
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            True,
            id="matching_columns_no_orderings",
        ),
        pytest.param(
            # Note: Root is the only node that cares about column ordering.
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            RelationalRoot(
                build_simple_scan(),
                [
                    ("b", make_relational_column_reference("b")),
                    ("a", make_relational_column_reference("a")),
                ],
            ),
            False,
            id="same_columns_different_indices",
        ),
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            RelationalRoot(
                build_simple_scan(),
                [
                    ("c", make_relational_column_reference("a")),
                    ("d", make_relational_column_reference("b")),
                ],
            ),
            False,
            id="different_columns_no_orderings",
        ),
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a"), ascending=True
                    ),
                ],
            ),
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a"), ascending=True
                    ),
                ],
            ),
            True,
            id="matching_columns_with_orderings",
        ),
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a"), ascending=True
                    ),
                ],
            ),
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("b"), ascending=True
                    ),
                ],
            ),
            False,
            id="different_orderings",
        ),
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a"), ascending=True
                    ),
                ],
            ),
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                [
                    make_relational_column_ordering(
                        make_relational_column_reference("a"), ascending=False
                    ),
                ],
            ),
            False,
            id="different_direction",
        ),
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            RelationalRoot(
                Scan("table2", {}),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            False,
            id="different_inputs",
        ),
        pytest.param(
            RelationalRoot(
                build_simple_scan(),
                [
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            Scan(
                "table2",
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
def test_root_equals(first_root: RelationalRoot, second_root: Relational, output: bool):
    assert first_root.equals(second_root) == output


def test_root_duplicate_columns():
    """
    Test to verify that we raise an error when the root node is
    created with duplicate column names.
    """
    with pytest.raises(AssertionError, match="Duplicate column names found in root."):
        RelationalRoot(
            build_simple_scan(),
            [
                ("a", make_relational_column_reference("a")),
                ("a", make_relational_column_reference("b")),
            ],
        )


@pytest.mark.parametrize(
    "join, output",
    [
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                LiteralExpression(True, BooleanType()),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            "JOIN(cond=Literal(value=True, type=BooleanType()), type=inner, columns={'a': Column(input=left, name=a, type=UnknownType()), 'b': Column(input=right, name=b, type=UnknownType())})",
            id="inner_join",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.LEFT,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            "JOIN(cond=Call(op=BinaryOperator[==], inputs=[Column(input=left, name=a, type=UnknownType()), Column(input=right, name=a, type=UnknownType())], return_type=BooleanType()), type=left, columns={'a': Column(input=left, name=a, type=UnknownType()), 'b': Column(input=right, name=b, type=UnknownType())})",
            id="left_join",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                LiteralExpression(False, BooleanType()),
                JoinType.RIGHT,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            "JOIN(cond=Literal(value=False, type=BooleanType()), type=right, columns={'a': Column(input=left, name=a, type=UnknownType()), 'b': Column(input=right, name=b, type=UnknownType())})",
            id="right_join",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("b", input_name="left"),
                        make_relational_column_reference("b", input_name="right"),
                    ],
                ),
                JoinType.FULL_OUTER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            "JOIN(cond=Call(op=BinaryOperator[==], inputs=[Column(input=left, name=b, type=UnknownType()), Column(input=right, name=b, type=UnknownType())], return_type=BooleanType()), type=full outer, columns={'a': Column(input=left, name=a, type=UnknownType()), 'b': Column(input=right, name=b, type=UnknownType())})",
            id="full_outer_join",
        ),
    ],
)
def test_join_to_string(join: Join, output: str):
    assert join.to_string() == output


@pytest.mark.parametrize(
    "first_join, second_join, output",
    [
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            True,
            id="same_columns",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "c": make_relational_column_reference("b", input_name="right"),
                },
            ),
            False,
            id="diff_columns",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            Join(
                build_simple_scan(),
                build_simple_scan(),
                # Note: We don't care that Equals commutes right now.
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="right"),
                        make_relational_column_reference("a", input_name="left"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            False,
            id="diff_conds",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.LEFT,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            False,
            id="diff_type",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            Join(
                Scan(
                    "table2",
                    {
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            False,
            id="different_left",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            Join(
                build_simple_scan(),
                Scan(
                    "table2",
                    {
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            False,
            id="different_right",
        ),
        pytest.param(
            Join(
                Scan(
                    "table2",
                    {
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            Join(
                build_simple_scan(),
                Scan(
                    "table2",
                    {
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            False,
            id="swapped_inputs",
        ),
        pytest.param(
            Join(
                build_simple_scan(),
                build_simple_scan(),
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                JoinType.INNER,
                {
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
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
            id="different_nodes",
        ),
    ],
)
def test_join_equals(first_join: Join, second_join: Relational, output: bool):
    assert first_join.equals(second_join) == output


def test_join_requires_boolean_condition():
    """
    Test to verify that we raise an error when the join node is
    created with a non-boolean condition.
    """
    with pytest.raises(AssertionError, match="Join condition must be a boolean type"):
        Join(
            build_simple_scan(),
            build_simple_scan(),
            make_relational_literal(1, Int64Type()),
            JoinType.INNER,
            {
                "a": make_relational_column_reference("a"),
            },
        )
