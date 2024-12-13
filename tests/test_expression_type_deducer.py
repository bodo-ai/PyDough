"""
Unit tests for the PyDough expression type inference utilities.

Copyright (C) 2024 Bodo Inc. All rights reserved.
"""

from collections.abc import MutableSequence

import pytest
from test_utils import AstNodeTestInfo, LiteralInfo

from pydough.pydough_ast import AstNodeBuilder, PyDoughAST
from pydough.pydough_ast import pydough_operators as pydop
from pydough.types import Int64Type, PyDoughType, StringType


@pytest.mark.parametrize(
    "deducer, args_info, expected_type",
    [
        pytest.param(
            pydop.ConstantType(StringType()),
            [],
            StringType(),
            id="always_string-empty_args",
        ),
        pytest.param(
            pydop.ConstantType(Int64Type()),
            [],
            Int64Type(),
            id="always_int64-empty_args",
        ),
        pytest.param(
            pydop.SelectArgumentType(0),
            [LiteralInfo(0, Int64Type()), LiteralInfo("foo", StringType())],
            Int64Type(),
            id="first_arg-int64_string",
        ),
        pytest.param(
            pydop.SelectArgumentType(1),
            [LiteralInfo(1, Int64Type()), LiteralInfo("foo", StringType())],
            StringType(),
            id="second_arg-int64_string",
        ),
    ],
)
def test_returned_type(
    deducer: pydop.ExpressionTypeDeducer,
    args_info: MutableSequence[AstNodeTestInfo],
    expected_type: PyDoughType,
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Checks that expression ttype deducers produce the correct type.
    """
    args: MutableSequence[PyDoughAST] = [
        info.build(tpch_node_builder) for info in args_info
    ]
    assert (
        deducer.infer_return_type(args) == expected_type
    ), "mismatch between inferred return type and expected type"
