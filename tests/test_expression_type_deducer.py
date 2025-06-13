"""
Unit tests for the PyDough expression type inference utilities.
"""

import pytest

import pydough.pydough_operators as pydop
from pydough.qdag import AstNodeBuilder, PyDoughQDAG
from pydough.types import NumericType, PyDoughType, StringType
from tests.testing_utilities import AstNodeTestInfo, LiteralInfo


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
            pydop.ConstantType(NumericType()),
            [],
            NumericType(),
            id="always_numeric-empty_args",
        ),
        pytest.param(
            pydop.SelectArgumentType(0),
            [LiteralInfo(0, NumericType()), LiteralInfo("foo", StringType())],
            NumericType(),
            id="first_arg-numeric_string",
        ),
        pytest.param(
            pydop.SelectArgumentType(1),
            [LiteralInfo(1, NumericType()), LiteralInfo("foo", StringType())],
            StringType(),
            id="second_arg-numeric_string",
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
