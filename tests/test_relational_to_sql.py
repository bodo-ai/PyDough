"""
Unit tests for converting our Relational nodes to generated SQL
via a SQLGlot intermediate.
"""

import sqlite3
from collections.abc import Callable

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

from pydough.database_connectors import DatabaseDialect
from pydough.pydough_operators import (
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
    RANKING,
    STARTSWITH,
    SUM,
    YEAR,
)
from pydough.relational import (
    Aggregate,
    CallExpression,
    EmptySingleton,
    Filter,
    Join,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    RelationalRoot,
    WindowCallExpression,
)
from pydough.sqlglot import SqlGlotTransformBindings, convert_relation_to_sql
from pydough.types import BooleanType, Int64Type, StringType, UnknownType


@pytest.fixture(scope="module")
def sqlite_dialect() -> SQLiteDialect:
    return SQLiteDialect()


@pytest.mark.parametrize(
    "root, test_name",
    [
        pytest.param(
            RelationalRoot(
                input=build_simple_scan(),
                ordered_columns=[("b", make_relational_column_reference("b"))],
            ),
            "simple_scan_test",
            id="simple_scan_test",
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
            "simple_scan_with_ordering",
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
            "project_scan_with_ordering",
            id="project_scan_with_ordering",
        ),
        pytest.param(
            RelationalRoot(
                input=Project(
                    input=EmptySingleton(),
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
            "simple_values",
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
            "simple_filter_test",
            id="simple_filter_test",
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
            "simple_limit",
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
            "duplicate_limit_min_inner",
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
            "duplicate_limit_min_outer",
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
            "duplicate_limit_different_ordering",
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
            "simple_limit_with_ordering",
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
            "simple_distinct",
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
            "simple_sum",
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
            "simple_groupby_sum",
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
            "simple_inner_join",
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
            "simple_left_join",
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
            "simple_right_join",
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
            "simple_full_outer_join",
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
            "simple_semi_join",
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
            "simple_anti_join",
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
            "nested_join",
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
            "nested_binary_functions",
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
            "ordering_function",
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
            "multi_join",
            id="multi_join",
        ),
    ],
)
def test_convert_relation_to_sqlite_sql(
    root: RelationalRoot,
    test_name: str,
    sqlite_dialect: SQLiteDialect,
    sqlite_bindings: SqlGlotTransformBindings,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Test converting a relational tree to SQL text in the SQLite dialect.
    """
    file_path: str = get_sql_test_filename(test_name, DatabaseDialect.SQLITE)
    created_sql: str = convert_relation_to_sql(
        root, sqlite_dialect, sqlite_bindings, True
    )
    if update_tests:
        with open(file_path, "w") as f:
            f.write(created_sql + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert (
            created_sql == expected_relational_string.strip()
        ), "Mismatch between tree generated SQL text and expected SQL text"


@pytest.mark.parametrize(
    "root, test_name",
    [
        pytest.param(
            tpch_query_1_plan(),
            "tpch_q1",
            id="tpch_q1",
        ),
        pytest.param(
            tpch_query_3_plan(),
            "tpch_q3",
            id="tpch_q3",
        ),
        pytest.param(
            tpch_query_6_plan(),
            "tpch_q6",
            id="tpch_q6",
        ),
    ],
)
def test_tpch_relational_to_sqlite_sql(
    root: RelationalRoot,
    test_name: str,
    sqlite_dialect: SQLiteDialect,
    sqlite_bindings: SqlGlotTransformBindings,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Test that we can take possible relational trees from select TPCH queries
    and convert them to reasonable SQL text. This will not be 1:1 in the result,
    but should be consistent SQL.

    These plans are generated from a couple simple plans we built with
    Apache Calcite in Bodo's SQL optimizer.
    """
    file_path: str = get_sql_test_filename(test_name, DatabaseDialect.SQLITE)
    created_sql: str = convert_relation_to_sql(
        root, sqlite_dialect, sqlite_bindings, True
    )
    if update_tests:
        with open(file_path, "w") as f:
            f.write(created_sql + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert (
            created_sql == expected_relational_string.strip()
        ), "Mismatch between tree generated SQL text and expected SQL text"


@pytest.mark.parametrize(
    "root, test_name",
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
            "starts_with",
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
            "ends_with",
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
            "contains",
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
            "isin",
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
            "like",
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
            "iff_iif",
            id="iff-iif",
            marks=pytest.mark.skipif(
                sqlite3.sqlite_version < "3.32.0",
                reason="SQLite 3.32.0 generates case statements for IFF",
            ),
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
            "iff_case",
            id="iff-case",
            marks=pytest.mark.skipif(
                sqlite3.sqlite_version >= "3.32.0", reason="SQLite 3.32.0 generates IFF"
            ),
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
            "year",
            id="year",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                    ("r", make_relational_column_reference("r")),
                ],
                input=Filter(
                    input=Filter(
                        input=Project(
                            input=build_simple_scan(),
                            columns={
                                "a": make_relational_column_reference("a"),
                                "b": make_relational_column_reference("b"),
                                "r": WindowCallExpression(
                                    RANKING,
                                    Int64Type(),
                                    [],
                                    [],
                                    [
                                        make_relational_ordering(
                                            make_relational_column_reference("a"),
                                            ascending=True,
                                            nulls_first=True,
                                        )
                                    ],
                                    {"allow_ties": True},
                                ),
                            },
                        ),
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                            "r": make_relational_column_reference("r"),
                        },
                        condition=CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("b"),
                                LiteralExpression(0, Int64Type()),
                            ],
                        ),
                    ),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                        "r": make_relational_column_reference("r"),
                    },
                    condition=CallExpression(
                        GEQ,
                        BooleanType(),
                        [
                            make_relational_column_reference("r"),
                            LiteralExpression(3, Int64Type()),
                        ],
                    ),
                ),
            ),
            "rank_with_filters_a",
            id="rank_with_filters_a",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[
                    ("a", make_relational_column_reference("a")),
                    ("b", make_relational_column_reference("b")),
                    ("r", make_relational_column_reference("r")),
                ],
                input=Filter(
                    input=Filter(
                        input=Project(
                            input=build_simple_scan(),
                            columns={
                                "a": make_relational_column_reference("a"),
                                "b": make_relational_column_reference("b"),
                                "r": WindowCallExpression(
                                    RANKING,
                                    Int64Type(),
                                    [],
                                    [],
                                    [
                                        make_relational_ordering(
                                            make_relational_column_reference("a"),
                                            ascending=True,
                                            nulls_first=True,
                                        )
                                    ],
                                    {"allow_ties": True},
                                ),
                            },
                        ),
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                            "r": make_relational_column_reference("r"),
                        },
                        condition=CallExpression(
                            GEQ,
                            BooleanType(),
                            [
                                make_relational_column_reference("r"),
                                LiteralExpression(3, Int64Type()),
                            ],
                        ),
                    ),
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                        "r": make_relational_column_reference("r"),
                    },
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("b"),
                            LiteralExpression(0, Int64Type()),
                        ],
                    ),
                ),
            ),
            "rank_with_filters_b",
            id="rank_with_filters_b",
        ),
    ],
)
def test_function_to_sql(
    root: RelationalRoot,
    test_name: str,
    sqlite_dialect: SQLiteDialect,
    sqlite_bindings: SqlGlotTransformBindings,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Tests that should be small as we need to just test converting a function
    to SQL.
    """
    file_path: str = get_sql_test_filename(f"func_{test_name}", DatabaseDialect.ANSI)
    created_sql: str = convert_relation_to_sql(
        root, sqlite_dialect, sqlite_bindings, True
    )
    if update_tests:
        with open(file_path, "w") as f:
            f.write(created_sql + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert (
            created_sql == expected_relational_string.strip()
        ), "Mismatch between tree generated SQL text and expected SQL text"
