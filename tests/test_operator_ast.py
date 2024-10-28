"""
TODO: add file-level docstring.
"""

from pydough.pydough_ast.pydough_operators.expression_operators.registered_expression_operators import (
    LOWER,
)
from pydough.types import PyDoughType, StringType
from typing import List, Tuple
from pydough.pydough_ast import PyDoughExpressionAST
from pydough.pydough_ast.expressions import ColumnProperty
from pydough.pydough_ast.pydough_operators import (
    PyDoughExpressionOperatorAST,
)
import pytest
from pydough.pydough_ast.expressions.expression_function_call import (
    ExpressionFunctionCall,
)

from pydough.metadata import (
    TableColumnMetadata,
)
from test_utils import graph_fetcher, get_table_column


@pytest.mark.parametrize(
    "operator, property_args, expected_type",
    [
        pytest.param(
            LOWER,
            [("TPCH", "Regions", "name")],
            StringType(),
            id="lower-single_string_arg",
        )
    ],
)
def test_call_return_type(
    get_sample_graph: graph_fetcher,
    operator: PyDoughExpressionOperatorAST,
    property_args: List[Tuple[str, str, str]],
    expected_type: PyDoughType,
):
    """
    Tests that function calls have the correct return type.
    """
    properties: List[ColumnProperty] = []
    for graph_name, collection_name, property_name in property_args:
        property: TableColumnMetadata = get_table_column(
            get_sample_graph, graph_name, collection_name, property_name
        )
        properties.append(ColumnProperty(property))
    call: PyDoughExpressionAST = ExpressionFunctionCall(LOWER, properties)
    return_type: PyDoughType = call.pydough_type
    assert (
        return_type == expected_type
    ), "Mismatch between return type and expected value"
