"""
TODO: add file-level docstring.
"""

from pydough.types import (
    PyDoughType,
    StringType,
    Float64Type,
    BooleanType,
    DateType,
    DecimalType,
)
from pydough.pydough_ast import AstNodeBuilder, ColumnProperty, ExpressionFunctionCall
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
def test_column_property_types(
    graph_name: str,
    property_info: AstNodeTestInfo,
    expected_type: PyDoughType,
    get_sample_graph: graph_fetcher,
):
    """
    Tests that column properties have the correct return type.
    """
    builder: AstNodeBuilder = AstNodeBuilder(get_sample_graph(graph_name))
    property: ColumnProperty = property_info.build(builder)
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
def test_call_return_type(
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
    call: ExpressionFunctionCall = call_info.build(builder)
    assert (
        call.pydough_type == expected_type
    ), "Mismatch between return type and expected value"
    assert (
        call.is_aggregation == out_aggregated
    ), "Mismatch between aggregation status and expected value"
