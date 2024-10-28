"""
TODO: add file-level docstring.
"""

from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.pydough_operators.expression_operators.registered_expression_operators import (
    LOWER,
    SUM,
)
from pydough.pydough_ast.pydough_operators.expression_operators.binary_operators import (
    BinaryOperator,
)
from pydough.pydough_ast.pydough_operators.expression_operators.expression_function_operators import (
    ExpressionFunctionCall,
)
import re
import pytest


def test_binop_wrong_num_args(binary_operators: BinaryOperator):
    """
    Verifies that every binary operator raises an appropriate exception
    when called with an insufficient number of arguments.
    """
    msg: str = f"Invalid operator invocation '? {binary_operators.binop.value} ?': Expected 2 arguments, received 0"
    with pytest.raises(PyDoughASTException, match=re.escape(msg)):
        binary_operators.verify_allows_args([])


@pytest.mark.parametrize(
    "function_operator",
    [
        pytest.param(LOWER, id="LOWER"),
        pytest.param(SUM, id="SUM"),
    ],
)
def test_function_wrong_num_args(function_operator: ExpressionFunctionCall):
    """
    Verifies that every function operator raises an appropriate exception
    when called with an insufficient number of arguments.
    """
    msg: str = f"Invalid operator invocation '{function_operator.function_name}()': Expected 1 argument, received 0"
    with pytest.raises(PyDoughASTException, match=re.escape(msg)):
        function_operator.verify_allows_args([])
