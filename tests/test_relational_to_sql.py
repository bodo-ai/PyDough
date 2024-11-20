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

from pydough.pydough_ast.pydough_operators import ADD, EQU, SUM
from pydough.relational.relational_expressions import CallExpression, LiteralExpression
from pydough.relational.relational_nodes import (
    Aggregate,
    Filter,
    Join,
    JoinType,
    Limit,
    Project,
    RelationalRoot,
)
from pydough.sqlglot import convert_relation_to_sql
from pydough.types import BooleanType, Int64Type


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
            "SELECT a, b FROM table WHERE a = 1",
            id="simple_filter",
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
            "SELECT a, b FROM table LIMIT 1",
            id="simple_limit",
        ),
        pytest.param(
            Limit(
                input=build_simple_scan(),
                limit=LiteralExpression(10, Int64Type()),
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
            "SELECT a, b FROM table ORDER BY a, b DESC LIMIT 10",
            id="simple_limit_with_ordering",
        ),
        pytest.param(
            Aggregate(
                input=build_simple_scan(),
                keys={
                    "b": make_relational_column_reference("b"),
                },
                aggregations={},
            ),
            "SELECT b FROM table GROUP BY b",
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
            "SELECT SUM(a) AS a FROM (SELECT a, b FROM table)",
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
            "SELECT b, SUM(a) AS a FROM (SELECT a, b FROM table) GROUP BY b",
            id="simple_groupby_sum",
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
            "SELECT _table_alias_0.a AS a, _table_alias_1.b AS b FROM (SELECT a, b FROM table) AS _table_alias_0 INNER JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="simple_inner_join",
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
                join_type=JoinType.LEFT,
                columns={
                    "a": make_relational_column_reference("a", input_name="left"),
                },
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 LEFT JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="simple_left_join",
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
                join_type=JoinType.RIGHT,
                columns={
                    "a": make_relational_column_reference("a", input_name="left"),
                },
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 RIGHT JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="simple_right_join",
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
                join_type=JoinType.FULL_OUTER,
                columns={
                    "a": make_relational_column_reference("a", input_name="left"),
                },
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 FULL OUTER JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="simple_full_outer_join",
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
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 WHERE EXISTS(SELECT 1 FROM (SELECT a, b FROM table) AS _table_alias_1 WHERE _table_alias_0.a = _table_alias_1.a)",
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
                    "a": make_relational_column_reference("a", input_name="left"),
                },
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 WHERE NOT EXISTS(SELECT 1 FROM (SELECT a, b FROM table) AS _table_alias_1 WHERE _table_alias_0.a = _table_alias_1.a)",
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
            "SELECT _table_alias_0.b AS d FROM (SELECT _table_alias_2.a AS a, _table_alias_3.b AS b FROM (SELECT a, b FROM table) AS _table_alias_2 INNER JOIN (SELECT a, b FROM table) AS _table_alias_3 ON _table_alias_2.a = _table_alias_3.a) AS _table_alias_0 LEFT JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="nested_join",
        ),
    ],
)
def test_convert_relation_to_sql(
    root: RelationalRoot, sql_text: str, sqlite_dialect: SQLiteDialect
):
    created_sql: str = convert_relation_to_sql(root, sqlite_dialect)
    assert created_sql == sql_text
