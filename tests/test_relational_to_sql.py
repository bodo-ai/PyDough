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

from pydough.pydough_ast.pydough_operators import (
    ADD,
    COUNT,
    DIV,
    EQU,
    LEQ,
    MUL,
    SUB,
    SUM,
)
from pydough.relational.relational_expressions import CallExpression, LiteralExpression
from pydough.relational.relational_nodes import (
    Aggregate,
    Filter,
    Join,
    JoinType,
    Limit,
    Project,
    RelationalRoot,
    Scan,
)
from pydough.sqlglot import convert_relation_to_sql
from pydough.types import BooleanType, DateType, Int64Type, UnknownType


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
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
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
            ),
            "SELECT a, b FROM table WHERE a = 1",
            id="simple_filter",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                input=Limit(
                    input=build_simple_scan(),
                    limit=LiteralExpression(1, Int64Type()),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
            ),
            "SELECT a, b FROM table LIMIT 1",
            id="simple_limit",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                input=Limit(
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
            ),
            "SELECT a, b FROM table ORDER BY a, b DESC LIMIT 10",
            id="simple_limit_with_ordering",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("b", make_relational_column_reference("b")),
                ],
                input=Aggregate(
                    input=build_simple_scan(),
                    keys={
                        "b": make_relational_column_reference("b"),
                    },
                    aggregations={},
                ),
            ),
            "SELECT b FROM table GROUP BY b",
            id="simple_distinct",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
                input=Aggregate(
                    input=build_simple_scan(),
                    keys={},
                    aggregations={
                        "a": CallExpression(
                            SUM, Int64Type(), [make_relational_column_reference("a")]
                        )
                    },
                ),
            ),
            "SELECT SUM(a) AS a FROM (SELECT a, b FROM table)",
            id="simple_sum",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
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
            ),
            "SELECT SUM(a) AS a, b FROM (SELECT a, b FROM table) GROUP BY b",
            id="simple_groupby_sum",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
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
            ),
            "SELECT _table_alias_0.a AS a, _table_alias_1.b AS b FROM (SELECT a, b FROM table) AS _table_alias_0 INNER JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="simple_inner_join",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
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
                    join_type=JoinType.LEFT,
                    columns={
                        "a": make_relational_column_reference("a", input_name="left"),
                    },
                ),
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 LEFT JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="simple_left_join",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
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
                    join_type=JoinType.RIGHT,
                    columns={
                        "a": make_relational_column_reference("a", input_name="left"),
                    },
                ),
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 RIGHT JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="simple_right_join",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
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
                    join_type=JoinType.FULL_OUTER,
                    columns={
                        "a": make_relational_column_reference("a", input_name="left"),
                    },
                ),
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 FULL OUTER JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="simple_full_outer_join",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
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
                    join_type=JoinType.SEMI,
                    columns={
                        "a": make_relational_column_reference("a", input_name="left"),
                    },
                ),
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 WHERE EXISTS(SELECT 1 FROM (SELECT a, b FROM table) AS _table_alias_1 WHERE _table_alias_0.a = _table_alias_1.a)",
            id="simple_semi_join",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
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
                    join_type=JoinType.ANTI,
                    columns={
                        "a": make_relational_column_reference("a", input_name="left"),
                    },
                ),
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 WHERE NOT EXISTS(SELECT 1 FROM (SELECT a, b FROM table) AS _table_alias_1 WHERE _table_alias_0.a = _table_alias_1.a)",
            id="simple_anti_join",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("d", make_relational_column_reference("d")),
                ],
                input=Join(
                    left=Join(
                        left=build_simple_scan(),
                        right=build_simple_scan(),
                        condition=CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference(
                                    "a", input_name="left"
                                ),
                                make_relational_column_reference(
                                    "a", input_name="right"
                                ),
                            ],
                        ),
                        join_type=JoinType.INNER,
                        columns={
                            "a": make_relational_column_reference(
                                "a", input_name="left"
                            ),
                            "b": make_relational_column_reference(
                                "b", input_name="right"
                            ),
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
            ),
            "SELECT _table_alias_0.b AS d FROM (SELECT _table_alias_2.a AS a, _table_alias_3.b AS b FROM (SELECT a, b FROM table) AS _table_alias_2 INNER JOIN (SELECT a, b FROM table) AS _table_alias_3 ON _table_alias_2.a = _table_alias_3.a) AS _table_alias_0 LEFT JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a",
            id="nested_join",
        ),
    ],
)
def test_convert_relation_to_sql(
    root: RelationalRoot, sql_text: str, sqlite_dialect: SQLiteDialect
):
    """
    Test converting a relational tree to SQL text in the SQLite dialect.
    """
    created_sql: str = convert_relation_to_sql(root, sqlite_dialect)
    assert created_sql == sql_text


@pytest.mark.parametrize(
    "root, sql_text",
    [
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("L_RETURNFLAG", make_relational_column_reference("L_RETURNFLAG")),
                    ("L_LINESTATUS", make_relational_column_reference("L_LINESTATUS")),
                    ("SUM_QTY", make_relational_column_reference("SUM_QTY")),
                    (
                        "SUM_BASE_PRICE",
                        make_relational_column_reference("SUM_BASE_PRICE"),
                    ),
                    (
                        "SUM_DISC_PRICE",
                        make_relational_column_reference("SUM_DISC_PRICE"),
                    ),
                    ("SUM_CHARGE", make_relational_column_reference("SUM_CHARGE")),
                    ("AVG_QTY", make_relational_column_reference("AVG_QTY")),
                    ("AVG_PRICE", make_relational_column_reference("AVG_PRICE")),
                    ("AVG_DISC", make_relational_column_reference("AVG_DISC")),
                    ("COUNT_ORDER", make_relational_column_reference("COUNT_ORDER")),
                ],
                orderings=[
                    make_relational_column_ordering(
                        make_relational_column_reference("L_RETURNFLAG"),
                        ascending=True,
                        nulls_first=True,
                    ),
                    make_relational_column_ordering(
                        make_relational_column_reference("L_LINESTATUS"),
                        ascending=True,
                        nulls_first=True,
                    ),
                ],
                input=Project(
                    columns={
                        "L_RETURNFLAG": make_relational_column_reference(
                            "L_RETURNFLAG"
                        ),
                        "L_LINESTATUS": make_relational_column_reference(
                            "L_LINESTATUS"
                        ),
                        "SUM_QTY": make_relational_column_reference("SUM_QTY"),
                        "SUM_BASE_PRICE": make_relational_column_reference(
                            "SUM_BASE_PRICE"
                        ),
                        "SUM_DISC_PRICE": make_relational_column_reference(
                            "SUM_DISC_PRICE"
                        ),
                        "SUM_CHARGE": make_relational_column_reference("SUM_CHARGE"),
                        "AVG_QTY": CallExpression(
                            DIV,
                            UnknownType(),
                            [
                                make_relational_column_reference("SUM_QTY"),
                                make_relational_column_reference("COUNT_ORDER"),
                            ],
                        ),
                        "AVG_PRICE": CallExpression(
                            DIV,
                            UnknownType(),
                            [
                                make_relational_column_reference("SUM_BASE_PRICE"),
                                make_relational_column_reference("COUNT_ORDER"),
                            ],
                        ),
                        "AVG_DISC": CallExpression(
                            DIV,
                            UnknownType(),
                            [
                                make_relational_column_reference("SUM_TAX"),
                                make_relational_column_reference("COUNT_ORDER"),
                            ],
                        ),
                        "COUNT_ORDER": make_relational_column_reference("COUNT_ORDER"),
                    },
                    input=Aggregate(
                        keys={
                            "L_RETURNFLAG": make_relational_column_reference(
                                "L_RETURNFLAG"
                            ),
                            "L_LINESTATUS": make_relational_column_reference(
                                "L_LINESTATUS"
                            ),
                        },
                        aggregations={
                            "SUM_QTY": CallExpression(
                                SUM,
                                UnknownType(),
                                [make_relational_column_reference("L_QUANTITY")],
                            ),
                            "SUM_BASE_PRICE": CallExpression(
                                SUM,
                                UnknownType(),
                                [make_relational_column_reference("L_EXTENDEDPRICE")],
                            ),
                            "SUM_TAX": CallExpression(
                                SUM,
                                UnknownType(),
                                [make_relational_column_reference("L_TAX")],
                            ),
                            "SUM_DISC_PRICE": CallExpression(
                                SUM,
                                UnknownType(),
                                [make_relational_column_reference("TEMP_COL0")],
                            ),
                            "SUM_CHARGE": CallExpression(
                                SUM,
                                UnknownType(),
                                [make_relational_column_reference("TEMP_COL1")],
                            ),
                            "COUNT_ORDER": CallExpression(
                                COUNT,
                                UnknownType(),
                                [],
                            ),
                        },
                        input=Project(
                            columns={
                                "L_QUANTITY": make_relational_column_reference(
                                    "L_QUANTITY"
                                ),
                                "L_EXTENDEDPRICE": make_relational_column_reference(
                                    "L_EXTENDEDPRICE"
                                ),
                                "L_TAX": make_relational_column_reference("L_TAX"),
                                "L_RETURNFLAG": make_relational_column_reference(
                                    "L_RETURNFLAG"
                                ),
                                "L_LINESTATUS": make_relational_column_reference(
                                    "L_LINESTATUS"
                                ),
                                "TEMP_COL0": make_relational_column_reference(
                                    "TEMP_COL0"
                                ),
                                "TEMP_COL1": CallExpression(
                                    MUL,
                                    UnknownType(),
                                    [
                                        make_relational_column_reference("TEMP_COL0"),
                                        CallExpression(
                                            ADD,
                                            UnknownType(),
                                            [
                                                make_relational_literal(1, Int64Type()),
                                                make_relational_column_reference(
                                                    "L_EXTENDEDPRICE"
                                                ),
                                            ],
                                        ),
                                    ],
                                ),
                            },
                            input=Project(
                                columns={
                                    "L_QUANTITY": make_relational_column_reference(
                                        "L_QUANTITY"
                                    ),
                                    "L_EXTENDEDPRICE": make_relational_column_reference(
                                        "L_EXTENDEDPRICE"
                                    ),
                                    "L_DISCOUNT": make_relational_column_reference(
                                        "L_DISCOUNT"
                                    ),
                                    "L_TAX": make_relational_column_reference("L_TAX"),
                                    "L_RETURNFLAG": make_relational_column_reference(
                                        "L_RETURNFLAG"
                                    ),
                                    "L_LINESTATUS": make_relational_column_reference(
                                        "L_LINESTATUS"
                                    ),
                                    "TEMP_COL0": CallExpression(
                                        MUL,
                                        UnknownType(),
                                        [
                                            make_relational_column_reference(
                                                "L_EXTENDEDPRICE"
                                            ),
                                            CallExpression(
                                                SUB,
                                                UnknownType(),
                                                [
                                                    make_relational_literal(
                                                        1, Int64Type()
                                                    ),
                                                    make_relational_column_reference(
                                                        "L_DISCOUNT"
                                                    ),
                                                ],
                                            ),
                                        ],
                                    ),
                                },
                                input=Filter(
                                    condition=CallExpression(
                                        LEQ,
                                        BooleanType(),
                                        [
                                            make_relational_column_reference(
                                                "L_SHIPDATE"
                                            ),
                                            # Note: This will be treated as a string literal
                                            # which is fine for now.
                                            make_relational_literal(
                                                "1998-12-01", DateType()
                                            ),
                                        ],
                                    ),
                                    columns={
                                        "L_QUANTITY": make_relational_column_reference(
                                            "L_QUANTITY"
                                        ),
                                        "L_EXTENDEDPRICE": make_relational_column_reference(
                                            "L_EXTENDEDPRICE"
                                        ),
                                        "L_DISCOUNT": make_relational_column_reference(
                                            "L_DISCOUNT"
                                        ),
                                        "L_TAX": make_relational_column_reference(
                                            "L_TAX"
                                        ),
                                        "L_RETURNFLAG": make_relational_column_reference(
                                            "L_RETURNFLAG"
                                        ),
                                        "L_LINESTATUS": make_relational_column_reference(
                                            "L_LINESTATUS"
                                        ),
                                    },
                                    input=Scan(
                                        table_name="LINEITEM",
                                        columns={
                                            "L_QUANTITY": make_relational_column_reference(
                                                "L_QUANTITY"
                                            ),
                                            "L_EXTENDEDPRICE": make_relational_column_reference(
                                                "L_EXTENDEDPRICE"
                                            ),
                                            "L_DISCOUNT": make_relational_column_reference(
                                                "L_DISCOUNT"
                                            ),
                                            "L_TAX": make_relational_column_reference(
                                                "L_TAX"
                                            ),
                                            "L_RETURNFLAG": make_relational_column_reference(
                                                "L_RETURNFLAG"
                                            ),
                                            "L_LINESTATUS": make_relational_column_reference(
                                                "L_LINESTATUS"
                                            ),
                                            "L_SHIPDATE": make_relational_column_reference(
                                                "L_SHIPDATE"
                                            ),
                                        },
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            "",
            id="tpch_q1",
        ),
    ],
)
def test_tpch_relational_to_sql(
    root: RelationalRoot, sql_text: str, sqlite_dialect: SQLiteDialect
):
    """
    Test that we can take possible relational trees from select TPCH queries
    and convert them to reasonable SQL text. This will not be 1:1 in the result,
    but should be consistent SQL.

    These plans are generated from a couple simple plans we built with
    Apache Calcite in Bodo's SQL optimizer.
    """
    created_sql: str = convert_relation_to_sql(root, sqlite_dialect)
    assert created_sql == sql_text
