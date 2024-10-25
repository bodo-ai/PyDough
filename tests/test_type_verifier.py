"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.pydough_ast import PyDoughAST
from pydough.pydough_ast.pydough_operators import TypeVerifier, AllowAny, RequireNumArgs
import pytest


@pytest.mark.parametrize(
    "verifier, args",
    [
        pytest.param(AllowAny(), [], id="allow_any-empty_args"),
        pytest.param(RequireNumArgs(0), [], id="require_0-empty_args"),
    ],
)
def test_verification(verifier: TypeVerifier, args: List[PyDoughAST]):
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
