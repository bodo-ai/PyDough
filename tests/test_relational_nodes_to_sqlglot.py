"""
Unit tests for converting our Relational nodes to the equivalent
SQLGlot expressions. This is just testing the conversion and is not
testing the actual runtime or converting entire complex trees.
"""

import pytest
from sqlglot.expressions import (
    EQ,
    GTE,
    Add,
    Expression,
    From,
    Length,
    Literal,
    Lower,
    Select,
    Sub,
    Sum,
    Table,
)
from sqlglot.expressions import Identifier as Ident
from test_utils import (
    build_simple_scan,
    make_relational_column_ordering,
    make_relational_column_reference,
    make_relational_literal,
)

from pydough.pydough_ast.pydough_operators import ADD, EQU, GEQ, LENGTH, LOWER, SUB, SUM
from pydough.relational.relational_expressions import CallExpression, LiteralExpression
from pydough.relational.relational_nodes import (
    Aggregate,
    Filter,
    Limit,
    Project,
    Relational,
    Scan,
)
from pydough.sqlglot import SQLGlotRelationalVisitor, find_identifiers
from pydough.types import BooleanType, Int64Type, StringType


@pytest.fixture(scope="module")
def sqlglot_relational_visitor() -> SQLGlotRelationalVisitor:
    return SQLGlotRelationalVisitor()


def set_alias(expr: Expression, alias: str) -> Expression:
    """
    Update and return the given expression with the given alias.
    This is used for expressions without the alias argument but who
    can't use set because we must return the original expression,
    for example in a fixture.

    Args:
        expr (Expression): The expression to update. This object is
            modified in place.
        alias (str): The alias name

    Returns:
        Expression: The updated expression.
    """
    expr.set("alias", alias)
    return expr


def mkglot(expressions: list[Expression], _from: Expression, **kwargs) -> Select:
    """
    Make a Select object with the given expressions and from clause and
    possibly some additional components. We require the expressions and
    from clause directly because all clauses must use them.

    Args:
        expressions (list[Expression]): The expressions to add as columns.
        _from (Expression): The from query that should not already be wrapped
        in a FROM.
    Kwargs:
        **kwargs: Additional keyword arguments that can be accepted
            to build the Select object. Examples include 'from',
            and 'where'.
    Returns:
        Select: The output select statement.
    """
    _from = From(this=_from)
    query: Select = Select(
        **{
            "expressions": expressions,
            "from": _from,
        }
    )
    if "where" in kwargs:
        query = query.where(kwargs.pop("where"))
    if "group_by" in kwargs:
        query = query.group_by(*kwargs.pop("group_by"))
    if "order_by" in kwargs:
        query = query.order_by(*kwargs.pop("order_by"))
    if "limit" in kwargs:
        query = query.limit(kwargs.pop("limit"))
    assert not kwargs, f"Unexpected keyword arguments: {kwargs}"
    return query


@pytest.mark.parametrize(
    "node, sqlglot_expr",
    [
        pytest.param(
            Scan(
                table_name="simple_scan",
                columns={
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="a"), Ident(this="b")],
                _from=Table(this=Ident(this="simple_scan")),
            ),
            id="simple_scan",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns={
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="a"), Ident(this="b")],
                _from=Table(this=Ident(this="table")),
            ),
            id="simple_project",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns={
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="b")],
                _from=Table(this=Ident(this="table")),
            ),
            id="column_pruning",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns={
                    "c": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[
                    set_alias(Ident(this="a"), "c"),
                    Ident(this="b"),
                ],
                _from=Table(this=Ident(this="table")),
            ),
            id="column_renaming",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns={
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                    "c": make_relational_literal(1),
                },
            ),
            mkglot(
                expressions=[
                    Ident(this="a"),
                    Ident(this="b"),
                    Literal(value=1, alias="c"),
                ],
                _from=Table(this=Ident(this="table")),
            ),
            id="literal_addition",
        ),
        pytest.param(
            Project(
                input=Project(
                    input=build_simple_scan(),
                    columns={
                        "col1": CallExpression(
                            LOWER, StringType(), [make_relational_column_reference("a")]
                        ),
                    },
                ),
                columns={
                    "col2": CallExpression(
                        LENGTH, Int64Type(), [make_relational_column_reference("col1")]
                    )
                },
            ),
            mkglot(
                expressions=[
                    set_alias(
                        Length.from_arg_list([Ident(this="col1")]),
                        "col2",
                    ),
                ],
                _from=mkglot(
                    expressions=[
                        set_alias(
                            Lower.from_arg_list([Ident(this="a")]),
                            "col1",
                        ),
                    ],
                    _from=mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=Table(this=Ident(this="table")),
                    ),
                ),
            ),
            id="repeated_functions",
        ),
        pytest.param(
            Filter(
                input=build_simple_scan(),
                columns={
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                condition=CallExpression(
                    EQU,
                    BooleanType(),
                    [make_relational_column_reference("a"), make_relational_literal(1)],
                ),
            ),
            mkglot(
                expressions=[Ident(this="a"), Ident(this="b")],
                _from=Table(this=Ident(this="table")),
                where=EQ(this=Ident(this="a"), expression=Literal(value=1)),
            ),
            id="simple_filter",
        ),
        pytest.param(
            Filter(
                input=Filter(
                    input=build_simple_scan(),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("a"),
                            make_relational_literal(1),
                        ],
                    ),
                ),
                columns={
                    "a": make_relational_column_reference("a"),
                },
                condition=CallExpression(
                    GEQ,
                    BooleanType(),
                    [make_relational_column_reference("b"), make_relational_literal(5)],
                ),
            ),
            mkglot(
                expressions=[Ident(this="a")],
                where=GTE(this=Ident(this="b"), expression=Literal(value=5)),
                _from=mkglot(
                    expressions=[Ident(this="a"), Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                    where=EQ(this=Ident(this="a"), expression=Literal(value=1)),
                ),
            ),
            id="nested_filters",
        ),
        pytest.param(
            Project(
                input=Filter(
                    input=Project(
                        input=build_simple_scan(),
                        columns={
                            "c": CallExpression(
                                ADD,
                                Int64Type(),
                                [
                                    make_relational_column_reference("a"),
                                    make_relational_literal(1, Int64Type()),
                                ],
                            ),
                            "b": make_relational_column_reference("b"),
                        },
                    ),
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("c"),
                            make_relational_literal(1),
                        ],
                    ),
                    columns={
                        "c": make_relational_column_reference("c"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                columns={
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=EQ(this=Ident(this="c"), expression=Literal(value=1)),
                _from=mkglot(
                    expressions=[
                        set_alias(
                            Add(this=Ident(this="a"), expression=Literal(value=1)),
                            "c",
                        ),
                        Ident(this="b"),
                    ],
                    _from=mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=Table(this=Ident(this="table")),
                    ),
                ),
            ),
            id="condition_pruning_project",
        ),
        pytest.param(
            Limit(
                input=build_simple_scan(),
                limit=LiteralExpression(1, Int64Type()),
                columns={
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="a"), Ident(this="b")],
                _from=Table(this=Ident(this="table")),
                limit=Literal(value=1),
            ),
            id="simple_limit",
        ),
        pytest.param(
            Limit(
                input=build_simple_scan(),
                limit=LiteralExpression(1, Int64Type()),
                columns={
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                },
                orderings=[
                    make_relational_column_ordering(
                        make_relational_column_reference("a"),
                        ascending=True,
                        nulls_first=True,
                    ),
                    make_relational_column_ordering(
                        make_relational_column_reference("b"),
                        ascending=False,
                        nulls_first=False,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="a"), Ident(this="b")],
                _from=Table(this=Ident(this="table")),
                order_by=[
                    Ident(this="a").asc(nulls_first=True),
                    Ident(this="b").desc(nulls_first=False),
                ],
                limit=Literal(value=1),
            ),
            id="simple_limit_with_ordering",
        ),
        pytest.param(
            Limit(
                input=Limit(
                    input=build_simple_scan(),
                    limit=LiteralExpression(5, Int64Type()),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                    orderings=[
                        make_relational_column_ordering(
                            make_relational_column_reference("b"),
                            ascending=True,
                            nulls_first=False,
                        ),
                    ],
                ),
                limit=LiteralExpression(2, Int64Type()),
                columns={
                    "a": make_relational_column_reference("a"),
                },
            ),
            mkglot(
                expressions=[Ident(this="a")],
                limit=Literal(value=2),
                _from=mkglot(
                    expressions=[Ident(this="a"), Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                    order_by=[Ident(this="b").asc(nulls_first=False)],
                    limit=Literal(value=5),
                ),
            ),
            id="nested_limits",
        ),
        pytest.param(
            Limit(
                input=Filter(
                    input=build_simple_scan(),
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("a"),
                            make_relational_literal(1),
                        ],
                    ),
                    columns={
                        "b": make_relational_column_reference("b"),
                    },
                ),
                limit=LiteralExpression(2, Int64Type()),
                columns={
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=EQ(this=Ident(this="a"), expression=Literal(value=1)),
                limit=Literal(value=2),
                _from=mkglot(
                    expressions=[Ident(this="a"), Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                ),
            ),
            id="filter_before_limit",
        ),
        pytest.param(
            Filter(
                input=Limit(
                    input=build_simple_scan(),
                    limit=LiteralExpression(2, Int64Type()),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                condition=CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a"),
                        make_relational_literal(1),
                    ],
                ),
                columns={
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=EQ(this=Ident(this="a"), expression=Literal(value=1)),
                _from=mkglot(
                    expressions=[Ident(this="a"), Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                    limit=Literal(value=2),
                ),
            ),
            id="limit_before_filter",
        ),
        pytest.param(
            Project(
                input=Limit(
                    input=build_simple_scan(),
                    limit=LiteralExpression(2, Int64Type()),
                    columns={
                        "b": make_relational_column_reference("b"),
                    },
                ),
                columns={
                    "b": make_relational_column_reference("b"),
                    "c": make_relational_literal(1),
                },
            ),
            mkglot(
                expressions=[Ident(this="b"), set_alias(Literal(value=1), "c")],
                _from=Table(this=Ident(this="table")),
                limit=Literal(value=2),
            ),
            id="project_limit_combine",
        ),
        pytest.param(
            Aggregate(
                input=build_simple_scan(),
                keys={
                    "b": make_relational_column_reference("b"),
                },
                aggregations={},
            ),
            mkglot(
                expressions=[Ident(this="b")],
                _from=Table(this=Ident(this="table")),
                group_by=[Ident(this="b")],
            ),
            id="simple_distinct",
        ),
        pytest.param(
            Aggregate(
                input=build_simple_scan(),
                keys={},
                aggregations={
                    "a": CallExpression(
                        SUM, Int64Type(), [make_relational_column_reference("a")]
                    )
                },
            ),
            mkglot(
                expressions=[
                    set_alias(
                        Sum.from_arg_list([Ident(this="a")]),
                        "a",
                    )
                ],
                _from=mkglot(
                    expressions=[Ident(this="a"), Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                ),
            ),
            id="simple_sum",
        ),
        pytest.param(
            Aggregate(
                input=build_simple_scan(),
                keys={
                    "b": make_relational_column_reference("b"),
                },
                aggregations={
                    "a": CallExpression(
                        SUM, Int64Type(), [make_relational_column_reference("a")]
                    )
                },
            ),
            mkglot(
                expressions=[
                    Ident(this="b"),
                    set_alias(
                        Sum.from_arg_list([Ident(this="a")]),
                        "a",
                    ),
                ],
                group_by=[Ident(this="b")],
                _from=mkglot(
                    expressions=[Ident(this="a"), Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                ),
            ),
            id="simple_groupby_sum",
        ),
        pytest.param(
            Aggregate(
                input=Filter(
                    input=build_simple_scan(),
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("a"),
                            make_relational_literal(1),
                        ],
                    ),
                    columns={
                        "b": make_relational_column_reference("b"),
                    },
                ),
                keys={
                    "b": make_relational_column_reference("b"),
                },
                aggregations={},
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=EQ(this=Ident(this="a"), expression=Literal(value=1)),
                group_by=[Ident(this="b")],
                _from=mkglot(
                    expressions=[Ident(this="a"), Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                ),
            ),
            id="filter_before_aggregate",
        ),
        pytest.param(
            Filter(
                input=Aggregate(
                    input=build_simple_scan(),
                    keys={
                        "b": make_relational_column_reference("b"),
                    },
                    aggregations={
                        "a": CallExpression(
                            SUM, Int64Type(), [make_relational_column_reference("a")]
                        )
                    },
                ),
                condition=CallExpression(
                    GEQ,
                    BooleanType(),
                    [
                        make_relational_column_reference("a"),
                        make_relational_literal(20),
                    ],
                ),
                columns={
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=GTE(this=Ident(this="a"), expression=Literal(value=20)),
                _from=mkglot(
                    expressions=[
                        Ident(this="b"),
                        set_alias(
                            Sum.from_arg_list([Ident(this="a")]),
                            "a",
                        ),
                    ],
                    group_by=[Ident(this="b")],
                    _from=mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=Table(this=Ident(this="table")),
                    ),
                ),
            ),
            id="filter_after_aggregate",
        ),
        pytest.param(
            Aggregate(
                input=Limit(
                    input=build_simple_scan(),
                    limit=LiteralExpression(10, Int64Type()),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                keys={
                    "b": make_relational_column_reference("b"),
                },
                aggregations={},
            ),
            mkglot(
                expressions=[Ident(this="b")],
                group_by=[Ident(this="b")],
                _from=mkglot(
                    expressions=[Ident(this="a"), Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                    limit=Literal(value=10),
                ),
            ),
            id="limit_before_aggregate",
        ),
        pytest.param(
            Limit(
                input=Aggregate(
                    input=build_simple_scan(),
                    keys={
                        "b": make_relational_column_reference("b"),
                    },
                    aggregations={},
                ),
                limit=LiteralExpression(10, Int64Type()),
                columns={
                    "b": make_relational_column_reference("b"),
                },
                orderings=[
                    make_relational_column_ordering(
                        make_relational_column_reference("b"),
                        ascending=False,
                        nulls_first=True,
                    )
                ],
            ),
            mkglot(
                expressions=[Ident(this="b")],
                _from=Table(this=Ident(this="table")),
                group_by=[Ident(this="b")],
                order_by=[Ident(this="b").desc(nulls_first=True)],
                limit=Literal(value=10),
            ),
            id="limit_after_aggregate",
        ),
        pytest.param(
            Project(
                input=Aggregate(
                    input=build_simple_scan(),
                    keys={
                        "b": make_relational_column_reference("b"),
                    },
                    aggregations={},
                ),
                columns={
                    "b": CallExpression(
                        SUB,
                        Int64Type(),
                        [
                            make_relational_column_reference("b"),
                            make_relational_literal(1, Int64Type()),
                        ],
                    ),
                },
            ),
            mkglot(
                expressions=[
                    set_alias(
                        Sub(
                            this=Ident(this="b"),
                            expression=Literal(value=1),
                        ),
                        "b",
                    ),
                ],
                _from=mkglot(
                    expressions=[Ident(this="b")],
                    group_by=[Ident(this="b")],
                    _from=Table(this=Ident(this="table")),
                ),
            ),
            id="project_after_aggregate",
        ),
    ],
)
def test_node_to_sqlglot(
    sqlglot_relational_visitor: SQLGlotRelationalVisitor,
    node: Relational,
    sqlglot_expr: Expression,
):
    """
    Test converting individual subtrees to SQLGlot starting
    from the given node.
    """
    # Note: We reset manually because we can't test full roots yet.
    sqlglot_relational_visitor.reset()
    node.accept(sqlglot_relational_visitor)
    actual = sqlglot_relational_visitor.get_sqlglot_result()
    assert actual == sqlglot_expr


@pytest.mark.parametrize(
    "expr, expected",
    [
        pytest.param(Ident(this="a"), {Ident(this="a")}, id="Ident"),
        pytest.param(Literal(this=1), set(), id="literal"),
        pytest.param(
            Add(
                this=Ident(this="a"),
                expression=Ident(this="b"),
            ),
            {Ident(this="a"), Ident(this="b")},
            id="function",
        ),
        pytest.param(
            Add(
                this=Ident(this="a"),
                expression=Add(this=Ident(this="b"), expression=Ident(this="c")),
            ),
            {Ident(this="a"), Ident(this="b"), Ident(this="c")},
            id="nested_function",
        ),
        pytest.param(
            Add(
                this=Ident(this="a"),
                expression=Add(this=Ident(this="b"), expression=Ident(this="a")),
            ),
            {Ident(this="a"), Ident(this="b")},
            id="duplicate_identifier",
        ),
    ],
)
def test_expression_identifiers(expr: Expression, expected: set[Ident]):
    assert find_identifiers(expr) == expected
