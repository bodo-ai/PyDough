"""
TODO: add file-level docstring.
"""

import re

import pytest
from test_utils import AstNodeTestInfo, FunctionInfo, graph_fetcher

from pydough.pydough_ast import (
    AstNodeBuilder,
    PyDoughASTException,
)
from pydough.pydough_ast import (
    pydough_operators as pydop,
)


def test_binop_wrong_num_args(binary_operators: pydop.BinaryOperator):
    """
    Verifies that every binary operator raises an appropriate exception
    when called with an insufficient number of arguments.
    """
    msg: str
    if binary_operators.binop in (pydop.BinOp.BAN, pydop.BinOp.BOR, pydop.BinOp.BXR):
        msg = f"Invalid operator invocation '? {binary_operators.binop.value} ?': Expected at least 2 arguments, received 0"
    else:
        msg = f"Invalid operator invocation '? {binary_operators.binop.value} ?': Expected 2 arguments, received 0"
    with pytest.raises(PyDoughASTException, match=re.escape(msg)):
        binary_operators.verify_allows_args([])


@pytest.mark.parametrize(
    "graph_name, call_info, error_string",
    [
        pytest.param(
            "TPCH",
            FunctionInfo("LOWER", []),
            "'LOWER()': Expected 1 argument, received 0",
            id="LOWER-0",
        ),
        pytest.param(
            "TPCH",
            FunctionInfo("SUM", []),
            "'SUM()': Expected 1 argument, received 0",
            id="SUM-0",
        ),
        pytest.param(
            "TPCH",
            FunctionInfo("IFF", []),
            "'IFF()': Expected 3 arguments, received 0",
            id="IFF-0",
        ),
    ],
)
def test_function_wrong_num_args(
    graph_name: str,
    call_info: AstNodeTestInfo,
    error_string: str,
    get_sample_graph: graph_fetcher,
):
    """
    Verifies that every function operator raises an appropriate exception
    when called with an insufficient number of arguments.
    """
    builder: AstNodeBuilder = AstNodeBuilder(get_sample_graph(graph_name))
    msg: str = f"Invalid operator invocation {error_string}"
    with pytest.raises(PyDoughASTException, match=re.escape(msg)):
        call_info.build(builder)
