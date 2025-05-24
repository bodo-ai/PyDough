"""
Unit tests the PyDough type verifiers.
"""

import pytest

import pydough.pydough_operators as pydop
from pydough.qdag import AstNodeBuilder, PyDoughQDAG
from pydough.types import NumericType
from tests.testing_utilities import AstNodeTestInfo, LiteralInfo


@pytest.mark.parametrize(
    "verifier, args_info",
    [
        pytest.param(pydop.AllowAny(), [], id="allow_any-empty_args"),
        pytest.param(pydop.RequireNumArgs(0), [], id="require_zero-empty_args"),
        pytest.param(
            pydop.RequireNumArgs(1),
            [LiteralInfo(16, NumericType())],
            id="require_one-one_arg",
        ),
        pytest.param(
            pydop.RequireNumArgs(2),
            [LiteralInfo(72, NumericType()), LiteralInfo(-1, NumericType())],
            id="require_two-two_args",
        ),
    ],
)
def test_verification(
    verifier: pydop.TypeVerifier,
    args_info: list[AstNodeTestInfo],
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Checks that verifiers accept certain arguments without raising an exception
    and also returns True.
    """
    args: list[PyDoughQDAG] = [info.build(tpch_node_builder) for info in args_info]
    assert verifier.accepts(args, error_on_fail=False), (
        "expected verifier to accept argument"
    )
    assert verifier.accepts(args, error_on_fail=True), (
        "expected verifier to accept argument"
    )
