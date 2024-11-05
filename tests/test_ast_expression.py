"""
TODO: add file-level docstring.
"""

from datetime import date
from pydough.types import (
    PyDoughType,
    StringType,
    Float64Type,
    BooleanType,
    DateType,
    Int64Type,
    DecimalType,
)
from pydough.pydough_ast import (
    PyDoughAST,
    AstNodeBuilder,
    ColumnProperty,
    ExpressionFunctionCall,
    Literal,
    PyDoughExpressionAST,
)
from test_utils import (
    graph_fetcher,
    AstNodeTestInfo,
    LiteralInfo,
    ColumnInfo,
    FunctionInfo,
)
import pytest


@pytest.mark.parametrize(
    "graph_name, property_info, expected_type",
    [
        pytest.param(
            "TPCH",
            ColumnInfo("Regions", "name"),
            StringType(),
            id="string",
        ),
        pytest.param(
            "Amazon",
            ColumnInfo("Products", "price_per_unit"),
            Float64Type(),
            id="float64",
        ),
        pytest.param(
            "TPCH",
            ColumnInfo("Lineitems", "ship_date"),
            DateType(),
            id="date",
        ),
    ],
)
def test_column_property_type(
    graph_name: str,
    property_info: AstNodeTestInfo,
    expected_type: PyDoughType,
    get_sample_graph: graph_fetcher,
):
    """
    Tests that column properties have the correct return type.
    """
    builder: AstNodeBuilder = AstNodeBuilder(get_sample_graph(graph_name))
    property: PyDoughAST = property_info.build(builder)
    assert isinstance(property, ColumnProperty)
    assert (
        property.pydough_type == expected_type
    ), "Mismatch between column property type and expected value"


@pytest.mark.parametrize(
    "graph_name, call_info, expected_type, out_aggregated",
    [
        pytest.param(
            "TPCH",
            FunctionInfo("LOWER", [ColumnInfo("Regions", "name")]),
            StringType(),
            False,
            id="lower-string",
        ),
        pytest.param(
            "Amazon",
            FunctionInfo("SUM", [ColumnInfo("Products", "price_per_unit")]),
            Float64Type(),
            True,
            id="sum-float64",
        ),
        pytest.param(
            "TPCH",
            FunctionInfo(
                "EQU",
                [
                    ColumnInfo("Lineitems", "ship_date"),
                    ColumnInfo("Lineitems", "receipt_date"),
                ],
            ),
            BooleanType(),
            False,
            id="equal-date-date",
        ),
        pytest.param(
            "TPCH",
            FunctionInfo(
                "IFF",
                [
                    LiteralInfo(True, BooleanType()),
                    ColumnInfo("Lineitems", "tax"),
                    ColumnInfo("Lineitems", "discount"),
                ],
            ),
            DecimalType(12, 2),
            False,
            id="iff-bool-decimal-decimal",
        ),
    ],
)
def test_function_call_return(
    graph_name: str,
    call_info: AstNodeTestInfo,
    expected_type: PyDoughType,
    out_aggregated: bool,
    get_sample_graph: graph_fetcher,
):
    """
    Tests that function calls have the correct return type.
    """
    builder: AstNodeBuilder = AstNodeBuilder(get_sample_graph(graph_name))
    call = call_info.build(builder)
    assert isinstance(call, ExpressionFunctionCall)
    assert (
        call.pydough_type == expected_type
    ), "Mismatch between return type and expected value"
    assert (
        call.is_aggregation == out_aggregated
    ), "Mismatch between aggregation status and expected value"


@pytest.mark.parametrize(
    "literal_info, expected_type",
    [
        pytest.param(
            LiteralInfo("hello", StringType()),
            StringType(),
            id="string",
        ),
        pytest.param(
            LiteralInfo(-1, Int64Type()),
            Int64Type(),
            id="int64",
        ),
        pytest.param(
            LiteralInfo(date(2024, 10, 28), DateType()),
            DateType(),
            id="date",
        ),
    ],
)
def test_literal_type(
    literal_info: AstNodeTestInfo,
    expected_type: PyDoughType,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Tests that literal expressions have the correct return type.
    """
    expr = literal_info.build(tpch_node_builder)
    assert isinstance(expr, Literal)
    assert (
        expr.pydough_type == expected_type
    ), "Mismatch between literal type and expected value"


@pytest.mark.parametrize(
    "expr_info, expected_string",
    [
        pytest.param(
            FunctionInfo("LOWER", [ColumnInfo("Regions", "name")]),
            "LOWER(Column[tpch.REGION.r_name])",
            id="regular_func",
        ),
        pytest.param(
            FunctionInfo("SUM", [ColumnInfo("Lineitems", "tax")]),
            "SUM(Column[tpch.LINEITEM.l_tax])",
            id="agg_func",
        ),
        pytest.param(
            FunctionInfo(
                "IFF",
                [
                    FunctionInfo(
                        "EQU",
                        [
                            ColumnInfo("Lineitems", "ship_date"),
                            ColumnInfo("Lineitems", "receipt_date"),
                        ],
                    ),
                    ColumnInfo("Lineitems", "tax"),
                    LiteralInfo(0, Int64Type()),
                ],
            ),
            "IFF(Column[tpch.LINEITEM.l_shipdate] == Column[tpch.LINEITEM.l_receiptdate], Column[tpch.LINEITEM.l_tax], 0)",
            id="nested_functions",
        ),
        pytest.param(
            FunctionInfo(
                "ADD",
                [
                    LiteralInfo(1, Int64Type()),
                    FunctionInfo(
                        "ADD",
                        [LiteralInfo(2, Int64Type()), LiteralInfo(3, Int64Type())],
                    ),
                ],
            ),
            "1 + (2 + 3)",
            id="nested_binops_a",
        ),
        pytest.param(
            FunctionInfo(
                "ADD",
                [
                    FunctionInfo(
                        "ADD",
                        [LiteralInfo(1, Int64Type()), LiteralInfo(2, Int64Type())],
                    ),
                    LiteralInfo(3, Int64Type()),
                ],
            ),
            "(1 + 2) + 3",
            id="nested_binops_b",
        ),
        pytest.param(
            FunctionInfo(
                "ADD",
                [
                    FunctionInfo(
                        "ADD",
                        [
                            FunctionInfo(
                                "ADD",
                                [
                                    LiteralInfo(1, Int64Type()),
                                    LiteralInfo(2, Int64Type()),
                                ],
                            ),
                            LiteralInfo(3, Int64Type()),
                        ],
                    ),
                    FunctionInfo(
                        "ADD",
                        [
                            LiteralInfo(4, Int64Type()),
                            FunctionInfo(
                                "ADD",
                                [
                                    LiteralInfo(5, Int64Type()),
                                    LiteralInfo(6, Int64Type()),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            "((1 + 2) + 3) + (4 + (5 + 6))",
            id="nested_binops_c",
        ),
        pytest.param(
            FunctionInfo(
                "DIV",
                [
                    LiteralInfo(1, Int64Type()),
                    FunctionInfo(
                        "ADD",
                        [LiteralInfo(2, Int64Type()), LiteralInfo(3, Int64Type())],
                    ),
                ],
            ),
            "1 / (2 + 3)",
            id="nested_binops_d",
        ),
        pytest.param(
            FunctionInfo(
                "MUL",
                [
                    FunctionInfo(
                        "ADD",
                        [LiteralInfo(1, Int64Type()), LiteralInfo(2, Int64Type())],
                    ),
                    LiteralInfo(3, Int64Type()),
                ],
            ),
            "(1 + 2) * 3",
            id="nested_binops_e",
        ),
        pytest.param(
            FunctionInfo(
                "SUB",
                [
                    FunctionInfo(
                        "MUL",
                        [
                            FunctionInfo(
                                "ADD",
                                [
                                    LiteralInfo(1, Int64Type()),
                                    LiteralInfo(2, Int64Type()),
                                ],
                            ),
                            LiteralInfo(3, Int64Type()),
                        ],
                    ),
                    FunctionInfo(
                        "ADD",
                        [
                            LiteralInfo(4, Int64Type()),
                            FunctionInfo(
                                "POW",
                                [
                                    LiteralInfo(5, Int64Type()),
                                    LiteralInfo(6, Int64Type()),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            "((1 + 2) * 3) - (4 + (5 ** 6))",
            id="nested_binops_f",
        ),
    ],
)
def test_expression_strings(
    expr_info: AstNodeTestInfo, expected_string: str, tpch_node_builder: AstNodeBuilder
):
    """
    Tests that expressions generate the expected string representation. Note,
    the column names seen here will essentially never be seen in actual string
    representations since they will be replaced with references to the columns.
    """
    expr = expr_info.build(tpch_node_builder)
    assert isinstance(expr, PyDoughExpressionAST)
    assert (
        expr.to_string() == expected_string
    ), "Mismatch between string representation and expected value"
