"""
Error-handling unit tests the PyDough type verifiers.
"""

import pytest

import pydough.pydough_operators as pydop
from pydough.qdag import (
    AstNodeBuilder,
    PyDoughQDAG,
    PyDoughQDAGException,
)
from pydough.types import NumericType
from tests.testing_utilities import AstNodeTestInfo, LiteralInfo


@pytest.mark.parametrize(
    "verifier, args_info, error_message",
    [
        pytest.param(
            pydop.RequireNumArgs(1),
            [],
            "Expected 1 argument, received 0",
            id="require_one-empty_args",
        ),
        pytest.param(
            pydop.RequireNumArgs(2),
            [],
            "Expected 2 arguments, received 0",
            id="require_two-empty_args",
        ),
        pytest.param(
            pydop.RequireNumArgs(3),
            [],
            "Expected 3 arguments, received 0",
            id="require_three-empty_args",
        ),
        pytest.param(
            pydop.RequireNumArgs(0),
            [LiteralInfo(0, NumericType())],
            "Expected 0 arguments, received 1",
            id="require_zero-one_arg",
        ),
        pytest.param(
            pydop.RequireNumArgs(1),
            [LiteralInfo(0, NumericType()), LiteralInfo(16, NumericType())],
            "Expected 1 argument, received 2",
            id="require_one-two_arg",
        ),
        pytest.param(
            pydop.RequireNumArgs(2),
            [LiteralInfo(3, NumericType())],
            "Expected 2 arguments, received 1",
            id="require_one-two_arg",
        ),
        pytest.param(
            pydop.RequireNumArgs(2),
            [
                LiteralInfo(10, NumericType()),
                LiteralInfo(-20, NumericType()),
                LiteralInfo(35, NumericType()),
            ],
            "Expected 2 arguments, received 3",
            id="require_one-two_arg",
        ),
        pytest.param(
            pydop.RequireMinArgs(1),
            [],
            "Expected at least 1 argument, received 0",
            id="require_min_one-empty_args",
        ),
        pytest.param(
            pydop.RequireArgRange(2, 3),
            [LiteralInfo(10, NumericType())],
            "Expected between 2 and 3 arguments inclusive, received 1.",
            id="require_two_three-one_arg",
        ),
        pytest.param(
            pydop.RequireArgRange(2, 3),
            [LiteralInfo(10, NumericType())] * 5,
            "Expected between 2 and 3 arguments inclusive, received 5.",
            id="require_two_three-five_args",
        ),
        pytest.param(
            pydop.RequireCollection(),
            [
                LiteralInfo(3, NumericType()),
            ],
            "Expected a collection as an argument, received an expression",
            id="require_collection-expression",
        ),
    ],
)
def test_verification(
    verifier: pydop.TypeVerifier,
    args_info: list[AstNodeTestInfo],
    error_message: str,
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Checks that verifiers accept reject by raising an exception
    and also returns False.
    """
    args: list[PyDoughQDAG] = [info.build(tpch_node_builder) for info in args_info]
    assert not verifier.accepts(args, error_on_fail=False), (
        "expected verifier to reject argument"
    )
    with pytest.raises(PyDoughQDAGException, match=error_message):
        verifier.accepts(args, error_on_fail=True)
