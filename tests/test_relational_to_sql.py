"""
Unit tests for converting our Relational nodes to generated SQL
via a SQLGlot intermediate.
"""

import pytest
from sqlglot.dialects.sqlite import SQLite as SQLiteDialect
from test_utils import (
    build_simple_scan,
    make_relational_column_ordering,
    make_relational_column_reference,
    make_relational_literal,
)

from pydough.pydough_ast.pydough_operators import ADD
from pydough.relational.relational_expressions import CallExpression
from pydough.relational.relational_nodes import (
    Project,
    RelationalRoot,
)
from pydough.sqlglot import convert_relation_to_sql
from pydough.types import Int64Type


@pytest.fixture(scope="module")
def sqlite_dialect() -> SQLiteDialect:
    return SQLiteDialect()


@pytest.mark.parametrize(
    "root, sql_text",
    [
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[("b", make_relational_column_reference("b"))],
            ),
            "SELECT b FROM table",
            id="simple_scan",
        ),
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                orderings=[
                    make_relational_column_ordering(
                        make_relational_column_reference("a"),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            "SELECT a, b FROM table ORDER BY a",
            id="simple_scan_with_ordering",
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
                    make_relational_column_ordering(
                        make_relational_column_reference("c"),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
            ),
            "SELECT b FROM (SELECT a, b, a + 1 AS c FROM (SELECT a, b FROM table)) ORDER BY c",
            id="project_scan_with_ordering",
        ),
    ],
)
def test_convert_relation_to_sql(
    root: RelationalRoot, sql_text: str, sqlite_dialect: SQLiteDialect
):
    created_sql: str = convert_relation_to_sql(root, sqlite_dialect)
    assert created_sql == sql_text
