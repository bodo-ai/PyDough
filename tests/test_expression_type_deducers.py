"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.pydough_operators import (
    ExpressionTypeDeducer,
    SelectArgumentType,
)
import re
import pytest


@pytest.mark.parametrize(
    "deducer, args, error_message",
    [
        pytest.param(
            SelectArgumentType(0),
            [],
            re.escape("Cannot select type of argument 0 out of []"),
            id="select_0-empty_args",
        ),
        pytest.param(
            SelectArgumentType(-1),
            [],
            re.escape("Cannot select type of argument -1 out of []"),
            id="select_invalid-empty_args",
        ),
    ],
)
def test_invalid_deduction(
    deducer: ExpressionTypeDeducer, args: List[PyDoughAST], error_message: str
):
    """
    Checks cases where calling an expression type deducer on a list of PyDough
    AST objects should raise an exception
    """
    with pytest.raises(PyDoughASTException, match=error_message):
        deducer.infer_return_type(args)
