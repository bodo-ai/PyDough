"""
TODO: add file-level docstring.
"""

from pydough.pydough_ast import PyDoughASTException
from pydough.pydough_ast.pydough_operators import (
    LOWER,
    SUM,
    IFF,
    BinaryOperator,
    ExpressionFunctionOperator,
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
    "function_operator, expected_num_args",
    [
        pytest.param(LOWER, 1, id="LOWER"),
        pytest.param(SUM, 1, id="SUM"),
        pytest.param(IFF, 3, id="IFF"),
    ],
)
def test_function_wrong_num_args(
    function_operator: ExpressionFunctionOperator, expected_num_args: int
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
