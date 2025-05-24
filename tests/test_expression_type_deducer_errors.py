"""
Error-handling unit tests for the PyDough expression type inference utilities.
"""

import re

import pytest

import pydough.pydough_operators as pydop
from pydough.qdag import (
    AstNodeBuilder,
    PyDoughQDAG,
    PyDoughQDAGException,
)
from pydough.types import StringType
from tests.testing_utilities import AstNodeTestInfo, LiteralInfo


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
    args_info: list[AstNodeTestInfo],
    error_message: str,
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Checks cases where calling an expression type deducer on a list of PyDough
    QDAG objects should raise an exception
    """
    args: list[PyDoughQDAG] = [info.build(tpch_node_builder) for info in args_info]
    with pytest.raises(PyDoughQDAGException, match=re.escape(error_message)):
        deducer.infer_return_type(args)
