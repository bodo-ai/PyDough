"""
TODO: add file-level docstring.
"""

from typing import MutableSequence
from pydough.pydough_ast import (
    PyDoughAST,
    PyDoughASTException,
    AstNodeBuilder,
    pydough_operators as pydop,
)
import re
import pytest
from pydough.types import StringType
from test_utils import AstNodeTestInfo, LiteralInfo


@pytest.mark.parametrize(
    "deducer, args_info, error_message",
    [
        pytest.param(
            pydop.SelectArgumentType(0),
            [],
            "Cannot select type of argument 0 out of []",
            id="select_zero-empty_args",
        ),
        pytest.param(
            pydop.SelectArgumentType(-1),
            [],
            "Cannot select type of argument -1 out of []",
            id="select_negative-empty_args",
        ),
        pytest.param(
            pydop.SelectArgumentType(1),
            [LiteralInfo("fizzbuzz", StringType())],
            "Cannot select type of argument 1 out of ['fizzbuzz']",
            id="select_one-one_arg",
        ),
    ],
)
def test_invalid_deduction(
    deducer: pydop.ExpressionTypeDeducer,
    args_info: MutableSequence[AstNodeTestInfo],
    error_message: str,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Checks cases where calling an expression type deducer on a list of PyDough
    AST objects should raise an exception
    """
    args: MutableSequence[PyDoughAST] = [
        info.build(tpch_node_builder) for info in args_info
    ]
    with pytest.raises(PyDoughASTException, match=re.escape(error_message)):
        deducer.infer_return_type(args)
