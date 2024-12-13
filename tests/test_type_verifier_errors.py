"""
Error-handling unit tests the PyDough type verifiers.

Copyright (C) 2024 Bodo Inc. All rights reserved.
"""

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
    ],
)
def test_verification(
    verifier: pydop.TypeVerifier,
    args_info: MutableSequence[AstNodeTestInfo],
    error_message: str,
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Checks that verifiers accept reject by raising an exception
    and also returns False.
    """
    args: MutableSequence[PyDoughAST] = [
        info.build(tpch_node_builder) for info in args_info
    ]
    assert not verifier.accepts(
        args, error_on_fail=False
    ), "expected verifier to reject argument"
    with pytest.raises(PyDoughASTException, match=error_message):
        verifier.accepts(args, error_on_fail=True)
