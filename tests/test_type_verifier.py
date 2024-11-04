"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.pydough_ast import PyDoughAST, AstNodeBuilder, pydough_operators as pydop
import pytest
from pydough.types import Int64Type
from test_utils import AstNodeTestInfo, LiteralInfo


@pytest.mark.parametrize(
    "verifier, args_info",
    [
        pytest.param(pydop.AllowAny(), [], id="allow_any-empty_args"),
        pytest.param(pydop.RequireNumArgs(0), [], id="require_zero-empty_args"),
        pytest.param(
            pydop.RequireNumArgs(1),
            [LiteralInfo(16, Int64Type())],
            id="require_one-one_arg",
        ),
        pytest.param(
            pydop.RequireNumArgs(2),
            [LiteralInfo(72, Int64Type()), LiteralInfo(-1, Int64Type())],
            id="require_two-two_args",
        ),
    ],
)
def test_verification(
    verifier: pydop.TypeVerifier,
    args_info: List[AstNodeTestInfo],
    tpch_node_builder: AstNodeBuilder,
):
    """
    Checks that verifiers accept certain arguments without raising an exception
    and also returns True.
    """
    args: List[PyDoughAST] = [info.build(tpch_node_builder) for info in args_info]
    assert verifier.accepts(
        args, error_on_fail=False
    ), "expected verifier to accept argument"
    assert verifier.accepts(
        args, error_on_fail=True
    ), "expected verifier to accept argument"
