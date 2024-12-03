"""
TODO: add file-level docstring.
"""

import re
from collections.abc import MutableSequence

import pytest
from test_utils import AstNodeTestInfo, LiteralInfo

from pydough.pydough_ast import (
    AstNodeBuilder,
    PyDoughAST,
    PyDoughASTException,
)
from pydough.pydough_ast import (
    pydough_operators as pydop,
)
from pydough.types import StringType


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
) -> None:
    """
    Checks cases where calling an expression type deducer on a list of PyDough
    AST objects should raise an exception
    """
    args: MutableSequence[PyDoughAST] = [
        info.build(tpch_node_builder) for info in args_info
    ]
    with pytest.raises(PyDoughASTException, match=re.escape(error_message)):
        deducer.infer_return_type(args)
