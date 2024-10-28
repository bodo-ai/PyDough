"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.pydough_ast import PyDoughAST, pydough_operators as pydop
import pytest


@pytest.mark.parametrize(
    "verifier, args",
    [
        pytest.param(pydop.AllowAny(), [], id="allow_any-empty_args"),
        pytest.param(pydop.RequireNumArgs(0), [], id="require_0-empty_args"),
    ],
)
def test_verification(verifier: pydop.TypeVerifier, args: List[PyDoughAST]):
    """
    Checks that verifiers accept certain arguments without raising an exception
    and also returns True.
    """
    assert verifier.accepts(
        args, error_on_fail=False
    ), "expected verifier to accept argument"
    assert verifier.accepts(
        args, error_on_fail=True
    ), "expected verifier to accept argument"
