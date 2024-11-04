"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.types import StringType, Int64Type, PyDoughType
from pydough.pydough_ast import PyDoughAST, AstNodeBuilder, pydough_operators as pydop
import pytest
from test_utils import AstNodeTestInfo, LiteralInfo


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
    args_info: List[AstNodeTestInfo],
    expected_type: PyDoughType,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Checks that expression ttype deducers produce the correct type.
    """
    args: List[PyDoughAST] = [info.build(tpch_node_builder) for info in args_info]
    assert (
        deducer.infer_return_type(args) == expected_type
    ), "mismatch between inferred return type and expected type"
