"""
Unit tests for converting our Relational nodes to the equivalent
SQLGlot expressions. This is just testing the conversion and is not
testing the actual runtime or converting entire complex trees.
"""

import pytest
import sqlglot.expressions as sqlglot_expressions
from conftest import (
    build_simple_scan,
    make_relational_column_reference,
    make_relational_literal,
)
from sqlglot.expressions import Expression, From, Identifier, Literal, Select, Table

from pydough.pydough_ast.pydough_operators import LENGTH, LOWER
from pydough.relational.relational_expressions import CallExpression
from pydough.relational.relational_nodes import (
    Project,
    Relational,
    Scan,
    SQLGlotRelationalVisitor,
)
from pydough.types import Int64Type, StringType


@pytest.fixture(scope="module")
def sqlglot_relational_visitor() -> SQLGlotRelationalVisitor:
    return SQLGlotRelationalVisitor()


def set_expression_alias(expr: Expression, alias: str) -> Expression:
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
            Select(
                **{
                    "expressions": [
                        Identifier(this="a"),
                        Identifier(this="b"),
                    ],
                    "from": From(this=Table(this=Identifier(this="simple_scan"))),
                }
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
            Select(
                **{
                    "expressions": [
                        Identifier(this="a"),
                        Identifier(this="b"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
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
            Select(
                **{
                    "expressions": [
                        Identifier(this="b"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
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
            Select(
                **{
                    "expressions": [
                        Identifier(this="a", alias="c"),
                        Identifier(this="b"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
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
            Select(
                **{
                    "expressions": [
                        Identifier(this="a"),
                        Identifier(this="b"),
                        Literal(value=1, alias="c"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
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
            Select(
                **{
                    "expressions": [
                        set_expression_alias(
                            sqlglot_expressions.Length.from_arg_list(
                                [Identifier(this="col1")]
                            ),
                            "col2",
                        ),
                    ],
                    "from": From(
                        this=Select(
                            **{
                                "expressions": [
                                    set_expression_alias(
                                        sqlglot_expressions.Lower.from_arg_list(
                                            [Identifier(this="a")]
                                        ),
                                        "col1",
                                    ),
                                ],
                                "from": From(
                                    this=Select(
                                        **{
                                            "expressions": [
                                                Identifier(this="a"),
                                                Identifier(this="b"),
                                            ],
                                            "from": From(
                                                this=Table(
                                                    this=Identifier(this="table")
                                                )
                                            ),
                                        }
                                    )
                                ),
                            }
                        )
                    ),
                }
            ),
            id="repeated_functions",
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
    assert sqlglot_relational_visitor.get_sqlglot_result() == sqlglot_expr
