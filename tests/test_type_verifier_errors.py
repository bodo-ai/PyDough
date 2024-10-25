"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.pydough_operators import TypeVerifier, RequireNumArgs
import pytest


@pytest.mark.parametrize(
    "verifier, args, error_message",
    [
        pytest.param(
            RequireNumArgs(1),
            [],
            "Expected 1 argument, received 0",
            id="require_1-empty_args",
        ),
        pytest.param(
            RequireNumArgs(2),
            [],
            "Expected 2 arguments, received 0",
            id="require_2-empty_args",
        ),
        pytest.param(
            RequireNumArgs(3),
            [],
            "Expected 3 arguments, received 0",
            id="require_3-empty_args",
        ),
    ],
)
def test_verification(
    verifier: TypeVerifier, args: List[PyDoughAST], error_message: str
):
    """
    Checks that verifiers accept reject by raising an exception
    and also returns True.
    """
    assert not verifier.accepts(
        args, error_on_fail=False
    ), "expected verifier to reject argument"
    with pytest.raises(PyDoughASTException, match=error_message):
        verifier.accepts(args, error_on_fail=True)
