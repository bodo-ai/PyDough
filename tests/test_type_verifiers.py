"""
TODO: add file-level docstring.
"""

from typing import List
from pydough.pydough_ast import (
    PyDoughAST,
    AstNodeBuilder,
    TypeVerifier,
    AllowAny,
    RequireNumArgs,
)
import pytest
from test_utils import graph_fetcher, AstNodeTestInfo, ColumnInfo


@pytest.mark.parametrize(
    "graph_name, verifier, args_info",
    [
        pytest.param("TPCH", AllowAny(), [], id="allow_any-empty_args"),
        pytest.param("TPCH", RequireNumArgs(0), [], id="num_args-empty_args"),
        pytest.param(
            "TPCH",
            RequireNumArgs(1),
            [ColumnInfo("Regions", "name")],
            id="num_args-one_arg",
        ),
        pytest.param(
            "TPCH",
            RequireNumArgs(2),
            [ColumnInfo("Regions", "key"), ColumnInfo("Regions", "comment")],
            id="num_args-two_args",
        ),
    ],
)
def test_verification(
    graph_name: str,
    verifier: TypeVerifier,
    args_info: List[AstNodeTestInfo],
    get_sample_graph: graph_fetcher,
):
    """
    Checks that verifiers accept certain arguments without raising an exception
    """
    builder: AstNodeBuilder = AstNodeBuilder(get_sample_graph(graph_name))
    args: List[PyDoughAST] = [info.build(builder) for info in args_info]
    verifier.accepts(args)
