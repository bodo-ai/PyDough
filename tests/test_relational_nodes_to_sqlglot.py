"""
Unit tests for converting our Relational nodes to the equivalent
SQLGlot expressions. This is just testing the conversion and is not
testing the actual runtime or converting entire complex trees.
"""

from dataclasses import dataclass
from typing import Any

import pytest
from sqlglot.expressions import (
    EQ,
    GTE,
    Abs,
    Add,
    Binary,
    Expression,
    From,
    Length,
    Literal,
    Lower,
    Select,
    Sub,
    Subquery,
    Sum,
    Table,
)
from sqlglot.expressions import Identifier as Ident
from test_utils import (
    build_simple_scan,
    make_relational_column_reference,
    make_relational_literal,
    make_relational_ordering,
)

from pydough.pydough_ast.pydough_operators import (
    ABS,
    ADD,
    EQU,
    GEQ,
    LENGTH,
    LOWER,
    SUB,
    SUM,
)
from pydough.relational import (
    Aggregate,
    CallExpression,
    Filter,
    Join,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    Relational,
    RelationalRoot,
    Scan,
)
from pydough.sqlglot import SQLGlotRelationalVisitor, find_identifiers, set_glot_alias
from pydough.types import BooleanType, Int64Type, StringType


@pytest.fixture(scope="module")
def sqlglot_relational_visitor() -> SQLGlotRelationalVisitor:
    return SQLGlotRelationalVisitor()


@dataclass
class GlotFrom:
    """Wrapper for all required from components."""

    input: Expression
    alias: str | None

    def __init__(self, input: Expression, alias: str | None = None) -> None:
        self.input = input
        self.alias = alias


@dataclass
class GlotJoin:
    """Wrapper for all required join components."""

    on: Expression
    right_query: GlotFrom
    join_type: str


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
    if isinstance(expr, Select):
        # TODO: Replace with set_glot_alias when we confirm
        # the correct behavior for Select objects.
        expr.set("alias", alias)
    else:
        expr = set_glot_alias(expr, alias)
    return expr


def mk_literal(value: Any, is_string: bool = False) -> Literal:
    """
    Make a literal expression with the given value.

    Args:
        value (int): The value to use for the literal.
        is_string (bool): Whether the literal should be a string.

    Returns:
        Literal: The output literal expression.
    """
    return Literal(this=str(value), is_string=is_string)


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
    from_value = _from.input
    from_alias = _from.alias
    if isinstance(from_value, Select):
        from_result = Subquery(this=from_value, alias=from_alias)
    else:
        assert from_alias is None, "Alias is only allowed for subqueries"
        from_result = from_value
    from_result = From(this=from_result)
    query: Select = Select(
        **{
            "expressions": expressions,
            "from": from_result,
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
    if "join" in kwargs:
        join = kwargs.pop("join")
        input_expr = Subquery(this=join.right_query.input, alias=join.right_query.alias)
        query = query.join(input_expr, on=join.on, join_type=join.join_type)
    assert not kwargs, f"Unexpected keyword arguments: {kwargs}"
    return query


def mkglot_func(op: type[Expression], args: list[Expression]) -> Expression:
    """
    Make a function call expression with the given operator and arguments.
    """
    if issubclass(op, Binary):
        assert len(args) == 2, "Binary functions require exactly 2 arguments"
        return op(this=args[0], expression=args[1])
    else:
        return op.from_arg_list(args)


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
                _from=GlotFrom(Table(this=Ident(this="simple_scan"))),
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
                _from=GlotFrom(Table(this=Ident(this="table"))),
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
                _from=GlotFrom(Table(this=Ident(this="table"))),
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
                _from=GlotFrom(Table(this=Ident(this="table"))),
            ),
            id="column_renaming",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns={
                    "a": make_relational_column_reference("a"),
                    "b": make_relational_column_reference("b"),
                    "c": make_relational_literal(1, Int64Type()),
                },
            ),
            mkglot(
                expressions=[
                    Ident(this="a"),
                    Ident(this="b"),
                    set_alias(mk_literal(1, False), "c"),
                ],
                _from=GlotFrom(Table(this=Ident(this="table"))),
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
                        mkglot_func(Length, [Ident(this="col1")]),
                        "col2",
                    ),
                ],
                _from=GlotFrom(
                    mkglot(
                        expressions=[
                            set_alias(
                                mkglot_func(Lower, [Ident(this="a")]),
                                "col1",
                            ),
                        ],
                        _from=GlotFrom(
                            mkglot(
                                expressions=[Ident(this="a"), Ident(this="b")],
                                _from=GlotFrom(Table(this=Ident(this="table"))),
                            )
                        ),
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
                    [
                        make_relational_column_reference("a"),
                        make_relational_literal(1, Int64Type()),
                    ],
                ),
            ),
            mkglot(
                expressions=[Ident(this="a"), Ident(this="b")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
                where=mkglot_func(EQ, [Ident(this="a"), mk_literal(1, False)]),
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
                            make_relational_literal(1, Int64Type()),
                        ],
                    ),
                ),
                columns={
                    "a": make_relational_column_reference("a"),
                },
                condition=CallExpression(
                    GEQ,
                    BooleanType(),
                    [
                        make_relational_column_reference("b"),
                        make_relational_literal(5, Int64Type()),
                    ],
                ),
            ),
            mkglot(
                expressions=[Ident(this="a")],
                where=mkglot_func(GTE, [Ident(this="b"), mk_literal(5, False)]),
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                        where=mkglot_func(EQ, [Ident(this="a"), mk_literal(1, False)]),
                    )
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
                            make_relational_literal(1, Int64Type()),
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
                where=mkglot_func(EQ, [Ident(this="c"), mk_literal(1, False)]),
                _from=GlotFrom(
                    mkglot(
                        expressions=[
                            set_alias(
                                mkglot_func(
                                    Add, [Ident(this="a"), mk_literal(1, False)]
                                ),
                                "c",
                            ),
                            Ident(this="b"),
                        ],
                        _from=GlotFrom(
                            mkglot(
                                expressions=[Ident(this="a"), Ident(this="b")],
                                _from=GlotFrom(Table(this=Ident(this="table"))),
                            )
                        ),
                    )
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
                _from=GlotFrom(Table(this=Ident(this="table"))),
                limit=mk_literal(1, False),
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
                    make_relational_ordering(
                        make_relational_column_reference("a"),
                        ascending=True,
                        nulls_first=True,
                    ),
                    make_relational_ordering(
                        make_relational_column_reference("b"),
                        ascending=False,
                        nulls_first=False,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="a"), Ident(this="b")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
                order_by=[
                    Ident(this="a").asc(nulls_first=True),
                    Ident(this="b").desc(nulls_first=False),
                ],
                limit=mk_literal(1, False),
            ),
            id="simple_limit_with_ordering",
        ),
        pytest.param(
            Limit(
                input=build_simple_scan(),
                limit=LiteralExpression(1, Int64Type()),
                columns={
                    "a": make_relational_column_reference("a"),
                },
                orderings=[
                    make_relational_ordering(
                        CallExpression(
                            ABS,
                            Int64Type(),
                            [make_relational_column_reference("a")],
                        ),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="a")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
                order_by=[
                    mkglot_func(Abs, [Ident(this="a")]).asc(nulls_first=True),
                ],
                limit=mk_literal(1, False),
            ),
            id="simple_limit_with_func_ordering",
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
                        make_relational_ordering(
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
                limit=mk_literal(2, False),
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                        order_by=[Ident(this="b").asc(nulls_first=False)],
                        limit=mk_literal(5, False),
                    )
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
                            make_relational_literal(1, Int64Type()),
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
                where=mkglot_func(EQ, [Ident(this="a"), mk_literal(1, False)]),
                limit=mk_literal(2, False),
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    )
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
                        make_relational_literal(1, Int64Type()),
                    ],
                ),
                columns={
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=mkglot_func(EQ, [Ident(this="a"), mk_literal(1, False)]),
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                        limit=mk_literal(2, False),
                    )
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
                    "c": make_relational_literal(1, Int64Type()),
                },
            ),
            mkglot(
                expressions=[Ident(this="b"), set_alias(mk_literal(1, False), "c")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
                limit=mk_literal(2, False),
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
                _from=GlotFrom(Table(this=Ident(this="table"))),
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
                        mkglot_func(Sum, [Ident(this="a")]),
                        "a",
                    )
                ],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    )
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
                        mkglot_func(Sum, [Ident(this="a")]),
                        "a",
                    ),
                ],
                group_by=[Ident(this="b")],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    )
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
                            make_relational_literal(1, Int64Type()),
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
                where=mkglot_func(EQ, [Ident(this="a"), mk_literal(1, False)]),
                group_by=[Ident(this="b")],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    )
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
                        make_relational_literal(20, Int64Type()),
                    ],
                ),
                columns={
                    "b": make_relational_column_reference("b"),
                },
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=mkglot_func(GTE, [Ident(this="a"), mk_literal(20, False)]),
                _from=GlotFrom(
                    mkglot(
                        expressions=[
                            Ident(this="b"),
                            set_alias(
                                mkglot_func(Sum, [Ident(this="a")]),
                                "a",
                            ),
                        ],
                        group_by=[Ident(this="b")],
                        _from=GlotFrom(
                            mkglot(
                                expressions=[Ident(this="a"), Ident(this="b")],
                                _from=GlotFrom(Table(this=Ident(this="table"))),
                            )
                        ),
                    )
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
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                        limit=mk_literal(10, False),
                    )
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
                    make_relational_ordering(
                        make_relational_column_reference("b"),
                        ascending=False,
                        nulls_first=True,
                    )
                ],
            ),
            mkglot(
                expressions=[Ident(this="b")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
                group_by=[Ident(this="b")],
                order_by=[Ident(this="b").desc(nulls_first=True)],
                limit=mk_literal(10, False),
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
                        mkglot_func(Sub, [Ident(this="b"), mk_literal(1, False)]),
                        "b",
                    ),
                ],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="b")],
                        group_by=[Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    )
                ),
            ),
            id="project_after_aggregate",
        ),
        pytest.param(
            Join(
                left=build_simple_scan(),
                right=build_simple_scan(),
                condition=CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                join_type=JoinType.INNER,
                columns={
                    "a": make_relational_column_reference("a", input_name="left"),
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            mkglot(
                expressions=[
                    set_alias(Ident(this="_table_alias_0.a"), "a"),
                    set_alias(Ident(this="_table_alias_1.b"), "b"),
                ],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    ),
                    alias="_table_alias_0",
                ),
                join=GlotJoin(
                    right_query=GlotFrom(
                        mkglot(
                            expressions=[Ident(this="a"), Ident(this="b")],
                            _from=GlotFrom(Table(this=Ident(this="table"))),
                        ),
                        alias="_table_alias_1",
                    ),
                    on=mkglot_func(
                        EQ,
                        [
                            Ident(this="_table_alias_0.a"),
                            Ident(this="_table_alias_1.a"),
                        ],
                    ),
                    join_type="inner",
                ),
            ),
            id="simple_join",
        ),
        pytest.param(
            Join(
                left=build_simple_scan(),
                right=build_simple_scan(),
                condition=CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                join_type=JoinType.SEMI,
                columns={
                    "a": make_relational_column_reference("a", input_name="left"),
                },
            ),
            mkglot(
                expressions=[
                    set_alias(Ident(this="_table_alias_0.a"), "a"),
                ],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    ),
                    alias="_table_alias_0",
                ),
                join=GlotJoin(
                    right_query=GlotFrom(
                        mkglot(
                            expressions=[Ident(this="a"), Ident(this="b")],
                            _from=GlotFrom(Table(this=Ident(this="table"))),
                        ),
                        alias="_table_alias_1",
                    ),
                    on=mkglot_func(
                        EQ,
                        [
                            Ident(this="_table_alias_0.a"),
                            Ident(this="_table_alias_1.a"),
                        ],
                    ),
                    join_type="semi",
                ),
            ),
            id="simple_semi_join",
        ),
        pytest.param(
            Join(
                left=build_simple_scan(),
                right=build_simple_scan(),
                condition=CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                join_type=JoinType.ANTI,
                columns={
                    "b": make_relational_column_reference("b", input_name="right"),
                },
            ),
            mkglot(
                expressions=[
                    set_alias(Ident(this="_table_alias_1.b"), "b"),
                ],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    ),
                    alias="_table_alias_0",
                ),
                join=GlotJoin(
                    right_query=GlotFrom(
                        mkglot(
                            expressions=[Ident(this="a"), Ident(this="b")],
                            _from=GlotFrom(Table(this=Ident(this="table"))),
                        ),
                        alias="_table_alias_1",
                    ),
                    on=mkglot_func(
                        EQ,
                        [
                            Ident(this="_table_alias_0.a"),
                            Ident(this="_table_alias_1.a"),
                        ],
                    ),
                    join_type="anti",
                ),
            ),
            id="simple_anti_join",
        ),
        pytest.param(
            Join(
                left=Join(
                    left=build_simple_scan(),
                    right=build_simple_scan(),
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("a", input_name="left"),
                            make_relational_column_reference("a", input_name="right"),
                        ],
                    ),
                    join_type=JoinType.INNER,
                    columns={
                        "a": make_relational_column_reference("a", input_name="left"),
                        "b": make_relational_column_reference("b", input_name="right"),
                    },
                ),
                right=build_simple_scan(),
                condition=CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("a", input_name="left"),
                        make_relational_column_reference("a", input_name="right"),
                    ],
                ),
                join_type=JoinType.LEFT,
                columns={
                    "d": make_relational_column_reference("b", input_name="left"),
                },
            ),
            mkglot(
                expressions=[set_alias(Ident(this="_table_alias_0.b"), "d")],
                _from=GlotFrom(
                    mkglot(
                        expressions=[
                            set_alias(Ident(this="_table_alias_2.a"), "a"),
                            set_alias(Ident(this="_table_alias_3.b"), "b"),
                        ],
                        _from=GlotFrom(
                            mkglot(
                                expressions=[Ident(this="a"), Ident(this="b")],
                                _from=GlotFrom(Table(this=Ident(this="table"))),
                            ),
                            alias="_table_alias_2",
                        ),
                        join=GlotJoin(
                            right_query=GlotFrom(
                                mkglot(
                                    expressions=[Ident(this="a"), Ident(this="b")],
                                    _from=GlotFrom(Table(this=Ident(this="table"))),
                                ),
                                alias="_table_alias_3",
                            ),
                            on=mkglot_func(
                                EQ,
                                [
                                    Ident(this="_table_alias_2.a"),
                                    Ident(this="_table_alias_3.a"),
                                ],
                            ),
                            join_type="inner",
                        ),
                    ),
                    alias="_table_alias_0",
                ),
                join=GlotJoin(
                    right_query=GlotFrom(
                        mkglot(
                            expressions=[Ident(this="a"), Ident(this="b")],
                            _from=GlotFrom(Table(this=Ident(this="table"))),
                        ),
                        alias="_table_alias_1",
                    ),
                    on=mkglot_func(
                        EQ,
                        [
                            Ident(this="_table_alias_0.a"),
                            Ident(this="_table_alias_1.a"),
                        ],
                    ),
                    join_type="left",
                ),
            ),
            id="nested_join",
        ),
        pytest.param(
            Filter(
                input=Join(
                    left=build_simple_scan(),
                    right=build_simple_scan(),
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("a", input_name="left"),
                            make_relational_column_reference("a", input_name="right"),
                        ],
                    ),
                    join_type=JoinType.INNER,
                    columns={
                        "a": make_relational_column_reference("a", input_name="left"),
                        "b": make_relational_column_reference("b", input_name="right"),
                    },
                ),
                condition=CallExpression(
                    GEQ,
                    BooleanType(),
                    [
                        make_relational_column_reference("a"),
                        make_relational_literal(5, Int64Type()),
                    ],
                ),
                columns={
                    "a": make_relational_column_reference("a"),
                },
            ),
            mkglot(
                expressions=[Ident(this="a")],
                where=mkglot_func(GTE, [Ident(this="a"), mk_literal(5, False)]),
                _from=GlotFrom(
                    mkglot(
                        expressions=[
                            set_alias(Ident(this="_table_alias_0.a"), "a"),
                            set_alias(Ident(this="_table_alias_1.b"), "b"),
                        ],
                        _from=GlotFrom(
                            mkglot(
                                expressions=[Ident(this="a"), Ident(this="b")],
                                _from=GlotFrom(Table(this=Ident(this="table"))),
                            ),
                            alias="_table_alias_0",
                        ),
                        join=GlotJoin(
                            right_query=GlotFrom(
                                mkglot(
                                    expressions=[Ident(this="a"), Ident(this="b")],
                                    _from=GlotFrom(Table(this=Ident(this="table"))),
                                ),
                                alias="_table_alias_1",
                            ),
                            on=mkglot_func(
                                EQ,
                                [
                                    Ident(this="_table_alias_0.a"),
                                    Ident(this="_table_alias_1.a"),
                                ],
                            ),
                            join_type="inner",
                        ),
                    )
                ),
            ),
            id="filter_after_join",
        ),
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                    ("a", make_relational_column_reference("a")),
                ],
            ),
            mkglot(
                expressions=[Ident(this="b"), Ident(this="a")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
            ),
            id="simple_scan_root",
        ),
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("a"),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="a"), Ident(this="b")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
                order_by=[Ident(this="a").asc(nulls_first=True)],
            ),
            id="simple_ordering_scan_root",
        ),
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
            ),
            mkglot(
                expressions=[Ident(this="a")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
            ),
            id="pruning_root",
        ),
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                ],
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("a"),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="b")],
                order_by=[Ident(this="a").asc(nulls_first=True)],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    )
                ),
            ),
            id="pruning_root_ordering_dependent",
        ),
        pytest.param(
            RelationalRoot(
                input=Project(
                    input=build_simple_scan(),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                        "c": CallExpression(
                            ADD,
                            Int64Type(),
                            [
                                make_relational_column_reference("a"),
                                make_relational_literal(1, Int64Type()),
                            ],
                        ),
                    },
                ),
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                ],
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("c"),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="b")],
                order_by=[Ident(this="c").asc(nulls_first=True)],
                _from=GlotFrom(
                    mkglot(
                        expressions=[
                            Ident(this="a"),
                            Ident(this="b"),
                            set_alias(
                                mkglot_func(
                                    Add, [Ident(this="a"), mk_literal(1, False)]
                                ),
                                "c",
                            ),
                        ],
                        _from=GlotFrom(
                            mkglot(
                                expressions=[Ident(this="a"), Ident(this="b")],
                                _from=GlotFrom(Table(this=Ident(this="table"))),
                            )
                        ),
                    )
                ),
            ),
            id="root_after_project",
        ),
        pytest.param(
            RelationalRoot(
                input=Filter(
                    input=build_simple_scan(),
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("a"),
                            make_relational_literal(1, Int64Type()),
                        ],
                    ),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=mkglot_func(EQ, [Ident(this="a"), mk_literal(1, False)]),
                _from=GlotFrom(Table(this=Ident(this="table"))),
            ),
            id="root_after_filter",
        ),
        pytest.param(
            RelationalRoot(
                input=Limit(
                    input=build_simple_scan(),
                    limit=LiteralExpression(10, Int64Type()),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                    orderings=[
                        make_relational_ordering(
                            make_relational_column_reference("b"),
                            ascending=True,
                            nulls_first=True,
                        ),
                    ],
                ),
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                    ("a", make_relational_column_reference("a")),
                ],
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("a"),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="b"), Ident(this="a")],
                order_by=[Ident(this="a").asc(nulls_first=True)],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                        order_by=[Ident(this="b").asc(nulls_first=True)],
                        limit=mk_literal(10, False),
                    )
                ),
            ),
            id="root_after_limit",
        ),
        pytest.param(
            RelationalRoot(
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
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("a"),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="a")],
                order_by=[Ident(this="a").asc(nulls_first=True)],
                _from=GlotFrom(
                    mkglot(
                        expressions=[
                            Ident(this="b"),
                            set_alias(
                                mkglot_func(Sum, [Ident(this="a")]),
                                "a",
                            ),
                        ],
                        group_by=[Ident(this="b")],
                        _from=GlotFrom(
                            mkglot(
                                expressions=[Ident(this="a"), Ident(this="b")],
                                _from=GlotFrom(Table(this=Ident(this="table"))),
                            )
                        ),
                    ),
                ),
            ),
            # Note: Can be heavily optimized by simplifying the generated SQL.
            id="root_after_aggregate",
        ),
        pytest.param(
            RelationalRoot(
                Join(
                    left=build_simple_scan(),
                    right=build_simple_scan(),
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("a", input_name="left"),
                            make_relational_column_reference("a", input_name="right"),
                        ],
                    ),
                    join_type=JoinType.INNER,
                    columns={
                        "a": make_relational_column_reference("a", input_name="left"),
                        "b": make_relational_column_reference("b", input_name="right"),
                    },
                ),
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            mkglot(
                expressions=[set_alias(Ident(this="_table_alias_1.b"), "b")],
                _from=GlotFrom(
                    mkglot(
                        expressions=[Ident(this="a"), Ident(this="b")],
                        _from=GlotFrom(Table(this=Ident(this="table"))),
                    ),
                    alias="_table_alias_0",
                ),
                join=GlotJoin(
                    right_query=GlotFrom(
                        mkglot(
                            expressions=[Ident(this="a"), Ident(this="b")],
                            _from=GlotFrom(Table(this=Ident(this="table"))),
                        ),
                        alias="_table_alias_1",
                    ),
                    on=mkglot_func(
                        EQ,
                        [
                            Ident(this="_table_alias_0.a"),
                            Ident(this="_table_alias_1.a"),
                        ],
                    ),
                    join_type="inner",
                ),
            ),
            id="root_after_join",
        ),
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
                orderings=[
                    make_relational_ordering(
                        CallExpression(
                            ABS,
                            Int64Type(),
                            [make_relational_column_reference("a")],
                        ),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            mkglot(
                expressions=[Ident(this="a")],
                order_by=[mkglot_func(Abs, [Ident(this="a")]).asc(nulls_first=True)],
                _from=GlotFrom(Table(this=Ident(this="table"))),
            ),
            id="root_with_expr_ordering",
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
        pytest.param(mk_literal(1, False), set(), id="literal"),
        pytest.param(
            mkglot_func(Add, [Ident(this="a"), Ident(this="b")]),
            {Ident(this="a"), Ident(this="b")},
            id="function",
        ),
        pytest.param(
            mkglot_func(
                Add,
                [Ident(this="a"), mkglot_func(Add, [Ident(this="b"), Ident(this="c")])],
            ),
            {Ident(this="a"), Ident(this="b"), Ident(this="c")},
            id="nested_function",
        ),
        pytest.param(
            mkglot_func(
                Add,
                [Ident(this="a"), mkglot_func(Add, [Ident(this="b"), Ident(this="a")])],
            ),
            {Ident(this="a"), Ident(this="b")},
            id="duplicate_identifier",
        ),
    ],
)
def test_expression_identifiers(expr: Expression, expected: set[Ident]):
    """
    Verify that we can properly find all of the identifiers in each expression.
    """
    assert find_identifiers(expr) == expected


@pytest.mark.parametrize(
    "root, sqlglot_expr",
    [
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                    ("a", make_relational_column_reference("a")),
                ],
            ),
            mkglot(
                expressions=[Ident(this="b"), Ident(this="a")],
                _from=GlotFrom(Table(this=Ident(this="table"))),
            ),
            id="simple_scan_root",
        ),
        pytest.param(
            RelationalRoot(
                input=Filter(
                    input=build_simple_scan(),
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("a"),
                            make_relational_literal(1, Int64Type()),
                        ],
                    ),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                ],
            ),
            mkglot(
                expressions=[Ident(this="b")],
                where=mkglot_func(EQ, [Ident(this="a"), mk_literal(1, False)]),
                _from=GlotFrom(Table(this=Ident(this="table"))),
            ),
            id="root_after_filter",
        ),
    ],
)
def test_relational_to_sqlglot(
    sqlglot_relational_visitor: SQLGlotRelationalVisitor,
    root: RelationalRoot,
    sqlglot_expr: Expression,
):
    """
    Test converting a root node to SQLGlot using
    relational_to_sqlglot
    """
    actual = sqlglot_relational_visitor.relational_to_sqlglot(root)
    assert actual == sqlglot_expr
