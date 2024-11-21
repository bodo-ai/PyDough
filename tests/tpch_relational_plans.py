"""
File that contains example possible relational plans that can are valid
to represent various TPC-H queries. These are stored to simplify other
tests that need to process relational plans.
"""

from test_utils import (
    make_relational_column_ordering,
    make_relational_column_reference,
    make_relational_literal,
)

from pydough.pydough_ast.pydough_operators import (
    ADD,
    AND,
    COUNT,
    DIV,
    GEQ,
    LEQ,
    LET,
    MUL,
    SUB,
    SUM,
)
from pydough.relational.relational_expressions import CallExpression
from pydough.relational.relational_nodes import (
    Aggregate,
    Filter,
    Project,
    RelationalRoot,
    Scan,
)
from pydough.types import BooleanType, DateType, DecimalType, Int64Type, UnknownType


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
                        make_relational_column_reference("SUM_TAX"),
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
                        "L_QUANTITY": make_relational_column_reference("L_QUANTITY"),
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
                                    make_relational_literal("1998-12-01", DateType()),
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
                        AND,
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
                                    make_relational_column_reference("L_EXTENDEDPRICE"),
                                    make_relational_literal("0.07", DecimalType(3, 2)),
                                ],
                            ),
                            CallExpression(
                                GEQ,
                                BooleanType(),
                                [
                                    make_relational_column_reference("L_EXTENDEDPRICE"),
                                    make_relational_literal("0.05", DecimalType(3, 2)),
                                ],
                            ),
                            CallExpression(
                                LET,
                                BooleanType(),
                                [
                                    make_relational_column_reference("L_SHIPDATE"),
                                    make_relational_literal("1995-01-01", DateType()),
                                ],
                            ),
                            CallExpression(
                                GEQ,
                                BooleanType(),
                                [
                                    make_relational_column_reference("L_SHIPDATE"),
                                    make_relational_literal("1994-01-01", DateType()),
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
