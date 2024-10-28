"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.types import StringType, Int64Type, PyDoughType
from pydough.pydough_ast import PyDoughAST, pydough_operators as pydop
import pytest


@pytest.mark.parametrize(
    "deducer, args, expected_type",
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
    ],
)
def test_returned_type(
    deducer: pydop.ExpressionTypeDeducer,
    args: List[PyDoughAST],
    expected_type: PyDoughType,
):
    """
    Checks that expression ttype deducers produce the correct type.
    """
    assert (
        deducer.infer_return_type(args) == expected_type
    ), "mismatch between inferred return type and expected type"
