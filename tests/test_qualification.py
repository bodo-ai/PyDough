"""
TODO: add file-level docstring.
"""

from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)

from pydough.pydough_ast import PyDoughAST
from pydough.unqualified import SUM, UnqualifiedNode, UnqualifiedRoot, qualify_node


def pydough_impl_01(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following PyDough snippet:
    ```
    TPCH.Nations(nation_name=name, total_balance=SUM(customers.acctbal))
    ```
    """
    return root.Nations(
        nation_name=root.name, total_balance=SUM(root.customers.acctbal)
    )


@pytest.mark.parametrize(
    "impl, answer_str",
    [
        pytest.param(
            pydough_impl_01,
            "Nations(nation_name=name, total_balance=SUM(customers.acctbal))",
            id="01",
        ),
    ],
)
def test_qualify_node_to_ast_string(
    impl: Callable[[UnqualifiedNode], UnqualifiedNode],
    answer_str: str,
    get_sample_graph: graph_fetcher,
):
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified AST version, with the correct string representation.
    """
    root: UnqualifiedNode = UnqualifiedRoot(get_sample_graph("TPCH"))
    unqualified: UnqualifiedNode = impl(root)
    qualified: PyDoughAST = qualify_node(unqualified)
    assert (
        repr(qualified) == answer_str
    ), "Mismatch between string representation of qualified node and expected AST string"
