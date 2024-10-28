"""
TODO: add file-level docstring.
"""

from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.pydough_operators.expression_operators.registered_expression_operators import (
    LOWER,
    SUM,
    IFF,
)
from pydough.pydough_ast.pydough_operators.expression_operators.binary_operators import (
    BinaryOperator,
)
from pydough.pydough_ast.pydough_operators.expression_operators.expression_function_operators import (
    ExpressionFunctionCall,
)
import re
import pytest
# from pydough.pydough_ast.expressions.expression_function_call import (
#     ExpressionFunctionCall,
# )

from pydough.metadata import (
    GraphMetadata,
    CollectionMetadata,
    PropertyMetadata,
    TableColumnMetadata,
    PyDoughMetadataException,
)
from test_utils import graph_fetcher


def get_table_column(
    get_sample_graph: graph_fetcher,
    graph_name: str,
    collection_name: str,
    property_name: str,
) -> TableColumnMetadata:
    """
    Fetches a table column property from one of the sample graphs.

    Args:
        `get_sample_graph`: the function used to fetch the graph.
        `graph_name`: the name of the graph to fetch.
        `collection_name`: the name of the desired collection from the graph.
        `property_name`: the name of the desired property

    Returns:
        The desired table column property.

    Raises:
        `PyDoughMetadataException` if the desired property is not a table
        column property or does not exist.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    collection: CollectionMetadata = graph.get_collection("Regions")
    property: PropertyMetadata = collection.get_property("name")
    if not isinstance(property, TableColumnMetadata):
        raise PyDoughMetadataException(
            f"Expected {property.error_name} to be a table column property"
        )
    return property


def test_missing_collection(get_sample_graph: graph_fetcher):
    """
    XXX
    """
    # property: TableColumnMetadata = get_table_column(get_sample_graph, "TPCH", "Regions", "name")


def test_binop_wrong_num_args(binary_operators: BinaryOperator):
    """
    Verifies that every binary operator raises an appropriate exception
    when called with an insufficient number of arguments.
    """
    msg: str = f"Invalid operator invocation '? {binary_operators.binop.value} ?': Expected 2 arguments, received 0"
    with pytest.raises(PyDoughASTException, match=re.escape(msg)):
        binary_operators.verify_allows_args([])


@pytest.mark.parametrize(
    "function_operator, expected_num_args",
    [
        pytest.param(LOWER, 1, id="LOWER"),
        pytest.param(SUM, 1, id="SUM"),
        pytest.param(IFF, 3, id="IFF"),
    ],
)
def test_function_wrong_num_args(
    function_operator: ExpressionFunctionCall, expected_num_args: int
):
    """
    Verifies that every function operator raises an appropriate exception
    when called with an insufficient number of arguments.
    """
    arg_str = (
        "1 argument" if expected_num_args == 1 else f"{expected_num_args} arguments"
    )
    msg: str = f"Invalid operator invocation '{function_operator.function_name}()': Expected {arg_str}, received 0"
    with pytest.raises(PyDoughASTException, match=re.escape(msg)):
        function_operator.verify_allows_args([])
