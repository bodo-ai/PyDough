"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.pydough_ast import PyDoughAST
from pydough.pydough_ast.pydough_operators import TypeVerifier, AllowAny
import pytest


@pytest.mark.parametrize(
    "verifier, args",
    [pytest.param(AllowAny(), [], id="allow_any-empty_args")],
)
def test_verification(verifier: TypeVerifier, args: List[PyDoughAST]):
    """
    Checks that verifiers accept certain arguments without raising an exception
    """
    verifier.accepts(args)
