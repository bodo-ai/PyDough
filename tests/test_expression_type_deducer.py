"""
Unit tests for the PyDough expression type inference utilities.
"""

import pytest
from test_utils import AstNodeTestInfo, LiteralInfo

import pydough.pydough_operators as pydop
from pydough.qdag import AstNodeBuilder, PyDoughQDAG
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
    args_info: list[AstNodeTestInfo],
    expected_type: PyDoughType,
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Checks that expression ttype deducers produce the correct type.
    """
    args: list[PyDoughQDAG] = [info.build(tpch_node_builder) for info in args_info]
    assert deducer.infer_return_type(args) == expected_type, (
        "mismatch between inferred return type and expected type"
    )
