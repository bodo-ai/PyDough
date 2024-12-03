"""
Unit tests for converting our Relational nodes to generated SQL
via a SQLGlot intermediate.
"""

import pytest
from sqlglot.dialects import SQLite as SQLiteDialect
from test_utils import (
    build_simple_scan,
    make_relational_column_reference,
    make_relational_literal,
    make_relational_ordering,
)
from tpch_relational_plans import (
    tpch_query_1_plan,
    tpch_query_3_plan,
    tpch_query_6_plan,
)

from pydough.pydough_ast.pydough_operators import (
    ABS,
    ADD,
    BAN,
    CONTAINS,
    ENDSWITH,
    EQU,
    GEQ,
    IFF,
    ISIN,
    LIKE,
    MUL,
    STARTSWITH,
    SUM,
    YEAR,
)
from pydough.relational import (
    Aggregate,
    CallExpression,
    EmptyValues,
    Filter,
    Join,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    RelationalRoot,
)
from pydough.sqlglot import convert_relation_to_sql
from pydough.types import BooleanType, Int64Type, StringType, UnknownType


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
                    make_relational_ordering(
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
                    make_relational_ordering(
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
                input=Project(
                    input=EmptyValues(),
                    columns={
                        "A": make_relational_literal(42, Int64Type()),
                        "B": make_relational_literal("foo", StringType()),
                    },
                ),
                ordered_columns=[
                    ("A", make_relational_column_reference("A")),
                    ("B", make_relational_column_reference("B")),
                ],
                orderings=[],
            ),
            "SELECT 42 AS A, 'foo' AS B FROM (VALUES ())",
            id="simple_values",
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
                    input=Limit(
                        input=build_simple_scan(),
                        limit=LiteralExpression(1, Int64Type()),
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                        },
                    ),
                    limit=LiteralExpression(5, Int64Type()),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
            ),
            "SELECT a, b FROM table LIMIT 1",
            id="duplicate_limit_min_inner",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                input=Limit(
                    input=Limit(
                        input=build_simple_scan(),
                        limit=LiteralExpression(5, Int64Type()),
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
                    limit=LiteralExpression(1, Int64Type()),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                ),
            ),
            "SELECT a, b FROM table ORDER BY a, b DESC LIMIT 1",
            id="duplicate_limit_min_outer",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                input=Limit(
                    input=Limit(
                        input=build_simple_scan(),
                        limit=LiteralExpression(5, Int64Type()),
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
                        ],
                    ),
                    limit=LiteralExpression(2, Int64Type()),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                    orderings=[
                        make_relational_ordering(
                            make_relational_column_reference("b"),
                            ascending=False,
                            nulls_first=False,
                        ),
                    ],
                ),
            ),
            "SELECT a, b FROM (SELECT a, b FROM table ORDER BY a LIMIT 5) ORDER BY b DESC LIMIT 2",
            id="duplicate_limit_different_ordering",
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
                    inputs=[build_simple_scan(), build_simple_scan()],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t1"),
                            ],
                        )
                    ],
                    join_types=[JoinType.INNER],
                    columns={
                        "a": make_relational_column_reference("a", input_name="t0"),
                        "b": make_relational_column_reference("b", input_name="t1"),
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
                    inputs=[build_simple_scan(), build_simple_scan()],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t1"),
                            ],
                        )
                    ],
                    join_types=[JoinType.LEFT],
                    columns={
                        "a": make_relational_column_reference("a", input_name="t0"),
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
                    inputs=[build_simple_scan(), build_simple_scan()],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t1"),
                            ],
                        )
                    ],
                    join_types=[JoinType.RIGHT],
                    columns={
                        "a": make_relational_column_reference("a", input_name="t0"),
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
                    inputs=[build_simple_scan(), build_simple_scan()],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t1"),
                            ],
                        )
                    ],
                    join_types=[JoinType.FULL_OUTER],
                    columns={
                        "a": make_relational_column_reference("a", input_name="t0"),
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
                    inputs=[build_simple_scan(), build_simple_scan()],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t1"),
                            ],
                        )
                    ],
                    join_types=[JoinType.SEMI],
                    columns={
                        "a": make_relational_column_reference("a", input_name="t0"),
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
                    inputs=[build_simple_scan(), build_simple_scan()],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t1"),
                            ],
                        )
                    ],
                    join_types=[JoinType.ANTI],
                    columns={
                        "a": make_relational_column_reference("a", input_name="t0"),
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
                    inputs=[
                        Join(
                            inputs=[build_simple_scan(), build_simple_scan()],
                            conditions=[
                                CallExpression(
                                    EQU,
                                    BooleanType(),
                                    [
                                        make_relational_column_reference(
                                            "a", input_name="t0"
                                        ),
                                        make_relational_column_reference(
                                            "a", input_name="t1"
                                        ),
                                    ],
                                )
                            ],
                            join_types=[JoinType.INNER],
                            columns={
                                "a": make_relational_column_reference(
                                    "a", input_name="t0"
                                ),
                                "b": make_relational_column_reference(
                                    "b", input_name="t1"
                                ),
                            },
                        ),
                        build_simple_scan(),
                    ],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t1"),
                            ],
                        )
                    ],
                    join_types=[JoinType.LEFT],
                    columns={
                        "d": make_relational_column_reference("b", input_name="t0"),
                    },
                ),
            ),
            "SELECT _table_alias_2.b AS d FROM (SELECT _table_alias_0.a AS a, _table_alias_1.b AS b FROM (SELECT a, b FROM table) AS _table_alias_0 INNER JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a) AS _table_alias_2 LEFT JOIN (SELECT a, b FROM table) AS _table_alias_3 ON _table_alias_2.a = _table_alias_3.a",
            id="nested_join",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                ],
                input=Project(
                    input=build_simple_scan(),
                    columns={
                        "a": CallExpression(
                            MUL,
                            UnknownType(),
                            [
                                make_relational_column_reference("a"),
                                CallExpression(
                                    ADD,
                                    UnknownType(),
                                    [
                                        make_relational_column_reference("b"),
                                        make_relational_literal(1, Int64Type()),
                                    ],
                                ),
                            ],
                        ),
                        "b": CallExpression(
                            ADD,
                            UnknownType(),
                            [
                                make_relational_column_reference("a"),
                                CallExpression(
                                    MUL,
                                    UnknownType(),
                                    [
                                        make_relational_column_reference("b"),
                                        make_relational_literal(1, Int64Type()),
                                    ],
                                ),
                            ],
                        ),
                    },
                ),
            ),
            "SELECT a * (b + 1) AS a, a + (b * 1) AS b FROM (SELECT a, b FROM table)",
            id="nested_binary_functions",
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
            "SELECT a FROM table ORDER BY ABS(a)",
            id="ordering_function",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                ],
                input=Join(
                    inputs=[
                        build_simple_scan(),
                        build_simple_scan(),
                        build_simple_scan(),
                    ],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t1"),
                            ],
                        ),
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("a", input_name="t0"),
                                make_relational_column_reference("a", input_name="t2"),
                            ],
                        ),
                    ],
                    join_types=[JoinType.INNER, JoinType.INNER],
                    columns={
                        "a": make_relational_column_reference("a", input_name="t0"),
                    },
                ),
            ),
            "SELECT _table_alias_0.a AS a FROM (SELECT a, b FROM table) AS _table_alias_0 INNER JOIN (SELECT a, b FROM table) AS _table_alias_1 ON _table_alias_0.a = _table_alias_1.a INNER JOIN (SELECT a, b FROM table) AS _table_alias_2 ON _table_alias_0.a = _table_alias_2.a",
            id="multi_join",
        ),
    ],
)
def test_convert_relation_to_sql(
    root: RelationalRoot, sql_text: str, sqlite_dialect: SQLiteDialect
) -> None:
    """
    Test converting a relational tree to SQL text in the SQLite dialect.
    """
    created_sql: str = convert_relation_to_sql(root, sqlite_dialect)
    assert created_sql == sql_text


@pytest.mark.parametrize(
    "root, sql_text",
    [
        pytest.param(
            tpch_query_1_plan(),
            "SELECT L_RETURNFLAG, L_LINESTATUS, SUM_QTY, SUM_BASE_PRICE, SUM_DISC_PRICE, SUM_CHARGE, CAST(SUM_QTY AS REAL) / COUNT_ORDER AS AVG_QTY, CAST(SUM_BASE_PRICE AS REAL) / COUNT_ORDER AS AVG_PRICE, CAST(SUM_DISCOUNT AS REAL) / COUNT_ORDER AS AVG_DISC, COUNT_ORDER FROM (SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS SUM_QTY, SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(L_DISCOUNT) AS SUM_DISCOUNT, SUM(TEMP_COL0) AS SUM_DISC_PRICE, SUM(TEMP_COL1) AS SUM_CHARGE, COUNT() AS COUNT_ORDER FROM (SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_RETURNFLAG, L_LINESTATUS, TEMP_COL0, TEMP_COL0 * (1 + L_TAX) AS TEMP_COL1 FROM (SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS TEMP_COL0 FROM (SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS FROM (SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE FROM LINEITEM) WHERE L_SHIPDATE <= '1998-12-01'))) GROUP BY L_RETURNFLAG, L_LINESTATUS) ORDER BY L_RETURNFLAG, L_LINESTATUS",
            id="tpch_q1",
        ),
        pytest.param(
            tpch_query_3_plan(),
            "SELECT L_ORDERKEY, REVENUE, O_ORDERDATE, O_SHIPPRIORITY FROM (SELECT L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY, SUM(REVENUE) AS REVENUE FROM (SELECT L_ORDERKEY, REVENUE, O_ORDERDATE, O_SHIPPRIORITY FROM (SELECT L_ORDERKEY, L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS REVENUE FROM (SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT FROM (SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE FROM LINEITEM) WHERE L_SHIPDATE > '1995-03-15')) INNER JOIN (SELECT O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY FROM (SELECT O_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY FROM ORDERS WHERE O_ORDERDATE < '1995-03-15') INNER JOIN (SELECT C_CUSTKEY FROM (SELECT C_CUSTKEY, C_MKTSEGMENT FROM CUSTOMER) WHERE C_MKTSEGMENT = 'BUILDING') ON O_CUSTKEY = C_CUSTKEY) ON L_ORDERKEY = O_ORDERKEY) GROUP BY L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY) ORDER BY REVENUE DESC, O_ORDERDATE, L_ORDERKEY LIMIT 10",
            id="tpch_q3",
        ),
        pytest.param(
            tpch_query_6_plan(),
            "SELECT SUM(TEMP_COL0) AS REVENUE FROM (SELECT L_EXTENDEDPRICE * L_DISCOUNT AS TEMP_COL0 FROM (SELECT L_EXTENDEDPRICE, L_DISCOUNT FROM (SELECT L_QUANTITY, L_DISCOUNT, L_EXTENDEDPRICE, L_SHIPDATE FROM LINEITEM) WHERE (L_QUANTITY < 24) AND (L_DISCOUNT <= 0.07) AND (L_DISCOUNT >= 0.05) AND (L_SHIPDATE < '1995-01-01') AND (L_SHIPDATE >= '1994-01-01')))",
            id="tpch_q6",
        ),
    ],
)
def test_tpch_relational_to_sql(
    root: RelationalRoot, sql_text: str, sqlite_dialect: SQLiteDialect
) -> None:
    """
    Test that we can take possible relational trees from select TPCH queries
    and convert them to reasonable SQL text. This will not be 1:1 in the result,
    but should be consistent SQL.

    These plans are generated from a couple simple plans we built with
    Apache Calcite in Bodo's SQL optimizer.
    """
    created_sql: str = convert_relation_to_sql(root, sqlite_dialect)
    assert created_sql == sql_text


@pytest.mark.parametrize(
    "root, sql_text",
    [
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Filter(
                    input=build_simple_scan(),
                    columns={
                        "b": make_relational_column_reference("b"),
                    },
                    condition=CallExpression(
                        BAN,
                        BooleanType(),
                        [
                            CallExpression(
                                STARTSWITH,
                                BooleanType(),
                                [
                                    make_relational_column_reference("b"),
                                    make_relational_literal("a", UnknownType()),
                                ],
                            ),
                            CallExpression(
                                STARTSWITH,
                                BooleanType(),
                                [
                                    make_relational_column_reference("b"),
                                    make_relational_column_reference("a"),
                                ],
                            ),
                        ],
                    ),
                ),
            ),
            "SELECT b FROM table WHERE (b LIKE '%a') AND (b LIKE ('%' || a))",
            id="starts_with",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Filter(
                    input=build_simple_scan(),
                    columns={
                        "b": make_relational_column_reference("b"),
                    },
                    condition=CallExpression(
                        BAN,
                        BooleanType(),
                        [
                            CallExpression(
                                ENDSWITH,
                                BooleanType(),
                                [
                                    make_relational_column_reference("b"),
                                    make_relational_literal("a", UnknownType()),
                                ],
                            ),
                            CallExpression(
                                ENDSWITH,
                                BooleanType(),
                                [
                                    make_relational_column_reference("b"),
                                    make_relational_column_reference("a"),
                                ],
                            ),
                        ],
                    ),
                ),
            ),
            "SELECT b FROM table WHERE (b LIKE 'a%') AND (b LIKE (a || '%'))",
            id="ends_with",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Filter(
                    input=build_simple_scan(),
                    columns={
                        "b": make_relational_column_reference("b"),
                    },
                    condition=CallExpression(
                        BAN,
                        BooleanType(),
                        [
                            CallExpression(
                                CONTAINS,
                                BooleanType(),
                                [
                                    make_relational_column_reference("b"),
                                    make_relational_literal("a", UnknownType()),
                                ],
                            ),
                            CallExpression(
                                CONTAINS,
                                BooleanType(),
                                [
                                    make_relational_column_reference("b"),
                                    make_relational_column_reference("a"),
                                ],
                            ),
                        ],
                    ),
                ),
            ),
            "SELECT b FROM table WHERE (b LIKE '%a%') AND (b LIKE ('%' || a || '%'))",
            id="contains",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Filter(
                    input=build_simple_scan(),
                    columns={
                        "b": make_relational_column_reference("b"),
                    },
                    condition=CallExpression(
                        ISIN,
                        BooleanType(),
                        [
                            make_relational_column_reference("b"),
                            make_relational_literal([1, 2, 3], UnknownType()),
                        ],
                    ),
                ),
            ),
            "SELECT b FROM table WHERE b IN (1, 2, 3)",
            id="isin",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Filter(
                    input=build_simple_scan(),
                    columns={
                        "b": make_relational_column_reference("b"),
                    },
                    condition=CallExpression(
                        LIKE,
                        BooleanType(),
                        [
                            make_relational_column_reference("b"),
                            make_relational_literal("%abc%efg%", StringType()),
                        ],
                    ),
                ),
            ),
            "SELECT b FROM table WHERE b LIKE '%abc%efg%'",
            id="like",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Project(
                    input=build_simple_scan(),
                    columns={
                        "a": CallExpression(
                            IFF,
                            Int64Type(),
                            [
                                CallExpression(
                                    GEQ,
                                    BooleanType(),
                                    [
                                        make_relational_column_reference("b"),
                                        make_relational_literal(0, Int64Type()),
                                    ],
                                ),
                                make_relational_literal("Positive", StringType()),
                                make_relational_literal("Negative", StringType()),
                            ],
                        ),
                    },
                ),
            ),
            "SELECT IIF(b >= 0, 'Positive', 'Negative') AS a FROM (SELECT a, b FROM table)",
            id="iff",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Project(
                    input=build_simple_scan(),
                    columns={
                        "a": CallExpression(
                            YEAR,
                            Int64Type(),
                            [make_relational_column_reference("a")],
                        ),
                    },
                ),
            ),
            "SELECT CAST(STRFTIME(%Y, a) AS INTEGER) AS a FROM (SELECT a, b FROM table)",
            id="year",
        ),
    ],
)
def test_function_to_sql(
    root: RelationalRoot, sql_text: str, sqlite_dialect: SQLiteDialect
) -> None:
    """
    Tests that should be small as we need to just test converting a function
    to SQL.
    """
    created_sql: str = convert_relation_to_sql(root, sqlite_dialect)
    assert created_sql == sql_text
