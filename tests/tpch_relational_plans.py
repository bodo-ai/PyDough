"""
File that contains example possible relational plans that can are valid
to represent various TPC-H queries. These are stored to simplify other
tests that need to process relational plans.
"""

from datetime import date

from test_utils import (
    make_relational_column_reference,
    make_relational_literal,
    make_relational_ordering,
)

from pydough.pydough_ast.pydough_operators import (
    ADD,
    BAN,
    COUNT,
    DIV,
    EQU,
    GEQ,
    GRT,
    LEQ,
    LET,
    MUL,
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
    Project,
    RelationalRoot,
    Scan,
)
from pydough.types import (
    BooleanType,
    DateType,
    DecimalType,
    Int64Type,
    StringType,
    UnknownType,
)


def tpch_query_1_plan() -> RelationalRoot:
    """
    Valid relational plan for TPC-H Query 1.
    """
    return RelationalRoot(
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
            make_relational_ordering(
                make_relational_column_reference("L_RETURNFLAG"),
                ascending=True,
                nulls_first=True,
            ),
            make_relational_ordering(
                make_relational_column_reference("L_LINESTATUS"),
                ascending=True,
                nulls_first=True,
            ),
        ],
        input=Project(
            columns={
                "L_RETURNFLAG": make_relational_column_reference("L_RETURNFLAG"),
                "L_LINESTATUS": make_relational_column_reference("L_LINESTATUS"),
                "SUM_QTY": make_relational_column_reference("SUM_QTY"),
                "SUM_BASE_PRICE": make_relational_column_reference("SUM_BASE_PRICE"),
                "SUM_DISC_PRICE": make_relational_column_reference("SUM_DISC_PRICE"),
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
                        make_relational_column_reference("SUM_DISCOUNT"),
                        make_relational_column_reference("COUNT_ORDER"),
                    ],
                ),
                "COUNT_ORDER": make_relational_column_reference("COUNT_ORDER"),
            },
            input=Aggregate(
                keys={
                    "L_RETURNFLAG": make_relational_column_reference("L_RETURNFLAG"),
                    "L_LINESTATUS": make_relational_column_reference("L_LINESTATUS"),
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
                    "SUM_DISCOUNT": CallExpression(
                        SUM,
                        UnknownType(),
                        [make_relational_column_reference("L_DISCOUNT")],
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
                        Int64Type(),
                        [],
                    ),
                },
                input=Project(
                    columns={
                        "L_QUANTITY": make_relational_column_reference("L_QUANTITY"),
                        "L_EXTENDEDPRICE": make_relational_column_reference(
                            "L_EXTENDEDPRICE"
                        ),
                        "L_DISCOUNT": make_relational_column_reference("L_DISCOUNT"),
                        "L_RETURNFLAG": make_relational_column_reference(
                            "L_RETURNFLAG"
                        ),
                        "L_LINESTATUS": make_relational_column_reference(
                            "L_LINESTATUS"
                        ),
                        "TEMP_COL0": make_relational_column_reference("TEMP_COL0"),
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
                                        make_relational_column_reference("L_TAX"),
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
                                    make_relational_column_reference("L_EXTENDEDPRICE"),
                                    CallExpression(
                                        SUB,
                                        UnknownType(),
                                        [
                                            make_relational_literal(1, Int64Type()),
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
                                    make_relational_column_reference("L_SHIPDATE"),
                                    # Note: This will be treated as a string literal
                                    # which is fine for now.
                                    make_relational_literal(
                                        date(1998, 12, 1), DateType()
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
                                "L_TAX": make_relational_column_reference("L_TAX"),
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
                                    "L_TAX": make_relational_column_reference("L_TAX"),
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
    )


def tpch_query_3_plan() -> RelationalRoot:
    """
    Valid relational plan for TPC-H Query 3.
    """
    # Note: Since this plan contains joins, to reduce the complexity
    # of each part we assign separate variables to each source table
    # and only put at most 1 join in each variable.
    customer = Filter(
        columns={
            "C_CUSTKEY": make_relational_column_reference("C_CUSTKEY"),
        },
        condition=CallExpression(
            EQU,
            BooleanType(),
            [
                make_relational_column_reference("C_MKTSEGMENT"),
                make_relational_literal("BUILDING", StringType()),
            ],
        ),
        input=Scan(
            table_name="CUSTOMER",
            columns={
                "C_CUSTKEY": make_relational_column_reference("C_CUSTKEY"),
                "C_MKTSEGMENT": make_relational_column_reference("C_MKTSEGMENT"),
            },
        ),
    )

    orders = Filter(
        columns={
            "O_CUSTKEY": make_relational_column_reference("O_CUSTKEY"),
            "O_ORDERKEY": make_relational_column_reference("O_ORDERKEY"),
            "O_ORDERDATE": make_relational_column_reference("O_ORDERDATE"),
            "O_SHIPPRIORITY": make_relational_column_reference("O_SHIPPRIORITY"),
        },
        condition=CallExpression(
            LET,
            BooleanType(),
            [
                make_relational_column_reference("O_ORDERDATE"),
                make_relational_literal(date(1995, 3, 15), DateType()),
            ],
        ),
        input=Scan(
            table_name="ORDERS",
            columns={
                "O_CUSTKEY": make_relational_column_reference("O_CUSTKEY"),
                "O_ORDERKEY": make_relational_column_reference("O_ORDERKEY"),
                "O_ORDERDATE": make_relational_column_reference("O_ORDERDATE"),
                "O_SHIPPRIORITY": make_relational_column_reference("O_SHIPPRIORITY"),
            },
        ),
    )

    customer_orders_join = Join(
        columns={
            "O_ORDERKEY": make_relational_column_reference(
                "O_ORDERKEY", input_name="t0"
            ),
            "O_ORDERDATE": make_relational_column_reference(
                "O_ORDERDATE", input_name="t0"
            ),
            "O_SHIPPRIORITY": make_relational_column_reference(
                "O_SHIPPRIORITY", input_name="t0"
            ),
        },
        inputs=[orders, customer],
        conditions=[
            CallExpression(
                EQU,
                BooleanType(),
                [
                    make_relational_column_reference("O_CUSTKEY", input_name="t0"),
                    make_relational_column_reference("C_CUSTKEY", input_name="t1"),
                ],
            )
        ],
        join_types=[JoinType.INNER],
    )

    lineitem = Project(
        columns={
            "L_ORDERKEY": make_relational_column_reference("L_ORDERKEY"),
            "REVENUE": CallExpression(
                MUL,
                UnknownType(),
                [
                    make_relational_column_reference("L_EXTENDEDPRICE"),
                    CallExpression(
                        SUB,
                        UnknownType(),
                        [
                            make_relational_literal(1, Int64Type()),
                            make_relational_column_reference("L_DISCOUNT"),
                        ],
                    ),
                ],
            ),
        },
        input=Filter(
            columns={
                "L_ORDERKEY": make_relational_column_reference("L_ORDERKEY"),
                "L_EXTENDEDPRICE": make_relational_column_reference("L_EXTENDEDPRICE"),
                "L_DISCOUNT": make_relational_column_reference("L_DISCOUNT"),
            },
            condition=CallExpression(
                GRT,
                BooleanType(),
                [
                    make_relational_column_reference("L_SHIPDATE"),
                    make_relational_literal(date(1995, 3, 15), DateType()),
                ],
            ),
            input=Scan(
                table_name="LINEITEM",
                columns={
                    "L_ORDERKEY": make_relational_column_reference("L_ORDERKEY"),
                    "L_EXTENDEDPRICE": make_relational_column_reference(
                        "L_EXTENDEDPRICE"
                    ),
                    "L_DISCOUNT": make_relational_column_reference("L_DISCOUNT"),
                    "L_SHIPDATE": make_relational_column_reference("L_SHIPDATE"),
                },
            ),
        ),
    )

    return RelationalRoot(
        ordered_columns=[
            ("L_ORDERKEY", make_relational_column_reference("L_ORDERKEY")),
            ("REVENUE", make_relational_column_reference("REVENUE")),
            ("O_ORDERDATE", make_relational_column_reference("O_ORDERDATE")),
            ("O_SHIPPRIORITY", make_relational_column_reference("O_SHIPPRIORITY")),
        ],
        orderings=[
            make_relational_ordering(
                make_relational_column_reference("REVENUE"),
                ascending=False,
                nulls_first=False,
            ),
            make_relational_ordering(
                make_relational_column_reference("O_ORDERDATE"),
                ascending=True,
                nulls_first=True,
            ),
            make_relational_ordering(
                make_relational_column_reference("L_ORDERKEY"),
                ascending=True,
                nulls_first=True,
            ),
        ],
        input=Limit(
            limit=make_relational_literal(10, Int64Type()),
            columns={
                "L_ORDERKEY": make_relational_column_reference("L_ORDERKEY"),
                "REVENUE": make_relational_column_reference("REVENUE"),
                "O_ORDERDATE": make_relational_column_reference("O_ORDERDATE"),
                "O_SHIPPRIORITY": make_relational_column_reference("O_SHIPPRIORITY"),
            },
            orderings=[
                make_relational_ordering(
                    make_relational_column_reference("REVENUE"),
                    ascending=False,
                    nulls_first=False,
                ),
                make_relational_ordering(
                    make_relational_column_reference("O_ORDERDATE"),
                    ascending=True,
                    nulls_first=True,
                ),
                make_relational_ordering(
                    make_relational_column_reference("L_ORDERKEY"),
                    ascending=True,
                    nulls_first=True,
                ),
            ],
            input=Aggregate(
                keys={
                    "L_ORDERKEY": make_relational_column_reference("L_ORDERKEY"),
                    "O_ORDERDATE": make_relational_column_reference("O_ORDERDATE"),
                    "O_SHIPPRIORITY": make_relational_column_reference(
                        "O_SHIPPRIORITY"
                    ),
                },
                aggregations={
                    "REVENUE": CallExpression(
                        SUM,
                        UnknownType(),
                        [make_relational_column_reference("REVENUE")],
                    ),
                },
                input=Join(
                    columns={
                        "L_ORDERKEY": make_relational_column_reference(
                            "L_ORDERKEY", input_name="t0"
                        ),
                        "REVENUE": make_relational_column_reference(
                            "REVENUE", input_name="t0"
                        ),
                        "O_ORDERDATE": make_relational_column_reference(
                            "O_ORDERDATE", input_name="t1"
                        ),
                        "O_SHIPPRIORITY": make_relational_column_reference(
                            "O_SHIPPRIORITY", input_name="t1"
                        ),
                    },
                    inputs=[lineitem, customer_orders_join],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference(
                                    "L_ORDERKEY", input_name="t0"
                                ),
                                make_relational_column_reference(
                                    "O_ORDERKEY", input_name="t1"
                                ),
                            ],
                        )
                    ],
                    join_types=[JoinType.INNER],
                ),
            ),
        ),
    )


def tpch_query_6_plan() -> RelationalRoot:
    """
    Valid relational plan for TPC-H Query 6.
    """
    return RelationalRoot(
        ordered_columns=[
            ("REVENUE", make_relational_column_reference("REVENUE")),
        ],
        input=Aggregate(
            keys={},
            aggregations={
                "REVENUE": CallExpression(
                    SUM,
                    UnknownType(),
                    [make_relational_column_reference("TEMP_COL0")],
                )
            },
            input=Project(
                columns={
                    "TEMP_COL0": CallExpression(
                        MUL,
                        UnknownType(),
                        [
                            make_relational_column_reference("L_EXTENDEDPRICE"),
                            make_relational_column_reference("L_DISCOUNT"),
                        ],
                    ),
                },
                input=Filter(
                    columns={
                        "L_EXTENDEDPRICE": make_relational_column_reference(
                            "L_EXTENDEDPRICE"
                        ),
                        "L_DISCOUNT": make_relational_column_reference("L_DISCOUNT"),
                    },
                    condition=CallExpression(
                        BAN,
                        BooleanType(),
                        [
                            CallExpression(
                                LET,
                                BooleanType(),
                                [
                                    make_relational_column_reference("L_QUANTITY"),
                                    make_relational_literal(24, Int64Type()),
                                ],
                            ),
                            CallExpression(
                                LEQ,
                                BooleanType(),
                                [
                                    make_relational_column_reference("L_DISCOUNT"),
                                    make_relational_literal("0.07", DecimalType(3, 2)),
                                ],
                            ),
                            CallExpression(
                                GEQ,
                                BooleanType(),
                                [
                                    make_relational_column_reference("L_DISCOUNT"),
                                    make_relational_literal("0.05", DecimalType(3, 2)),
                                ],
                            ),
                            CallExpression(
                                LET,
                                BooleanType(),
                                [
                                    make_relational_column_reference("L_SHIPDATE"),
                                    make_relational_literal(
                                        date(1995, 1, 1), DateType()
                                    ),
                                ],
                            ),
                            CallExpression(
                                GEQ,
                                BooleanType(),
                                [
                                    make_relational_column_reference("L_SHIPDATE"),
                                    make_relational_literal(
                                        date(1994, 1, 1), DateType()
                                    ),
                                ],
                            ),
                        ],
                    ),
                    input=Scan(
                        table_name="LINEITEM",
                        columns={
                            "L_QUANTITY": make_relational_column_reference(
                                "L_QUANTITY"
                            ),
                            "L_DISCOUNT": make_relational_column_reference(
                                "L_DISCOUNT"
                            ),
                            "L_EXTENDEDPRICE": make_relational_column_reference(
                                "L_EXTENDEDPRICE"
                            ),
                            "L_SHIPDATE": make_relational_column_reference(
                                "L_SHIPDATE"
                            ),
                        },
                    ),
                ),
            ),
        ),
    )
