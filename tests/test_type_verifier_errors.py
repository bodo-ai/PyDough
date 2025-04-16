"""
Error-handling unit tests the PyDough type verifiers.
"""

import pytest
from test_utils import AstNodeTestInfo, LiteralInfo

import pydough.pydough_operators as pydop
from pydough.qdag import (
    AstNodeBuilder,
    PyDoughQDAG,
    PyDoughQDAGException,
)
from pydough.types import Int64Type


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
            [LiteralInfo(0, Int64Type())],
            "Expected 0 arguments, received 1",
            id="require_zero-one_arg",
        ),
        pytest.param(
            pydop.RequireNumArgs(1),
            [LiteralInfo(0, Int64Type()), LiteralInfo(16, Int64Type())],
            "Expected 1 argument, received 2",
            id="require_one-two_arg",
        ),
        pytest.param(
            pydop.RequireNumArgs(2),
            [LiteralInfo(3, Int64Type())],
            "Expected 2 arguments, received 1",
            id="require_one-two_arg",
        ),
        pytest.param(
            pydop.RequireNumArgs(2),
            [
                LiteralInfo(10, Int64Type()),
                LiteralInfo(-20, Int64Type()),
                LiteralInfo(35, Int64Type()),
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
            [LiteralInfo(10, Int64Type())],
            "Expected between 2 and 3 arguments inclusive, received 1.",
            id="require_two_three-one_arg",
        ),
        pytest.param(
            pydop.RequireArgRange(2, 3),
            [LiteralInfo(10, Int64Type())] * 5,
            "Expected between 2 and 3 arguments inclusive, received 5.",
            id="require_two_three-five_args",
        ),
        pytest.param(
            pydop.RequireCollection(),
            [
                LiteralInfo(3, Int64Type()),
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
