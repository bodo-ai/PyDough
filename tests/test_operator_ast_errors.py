"""
TODO: add file-level docstring.
"""

from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.pydough_operators import PyDoughOperatorAST
import re
import pytest


def test_binop_wrong_num_args(binary_operators: PyDoughOperatorAST):
    """
    Verifies that every binary operator raises an apropriate exception
    when called with an insufficient number of arguments.
    """
    msg: str = f"Invalid operator invocation '? {binary_operators.binop.value} ?': Expected 2 arguments, received 0"
    with pytest.raises(PyDoughASTException, match=re.escape(msg)):
        binary_operators.verify_allows_args([])
