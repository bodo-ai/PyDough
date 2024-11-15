"""
TODO: add file-level docstring.
"""

import re
from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)

from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)


def bad_pydough_impl_01(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations(nation_name=name, total_balance=SUM(acctbal))
    ```
    """
    return root.Nations(nation_name=root.name, total_balance=root.SUM(root.acctbal))


@pytest.mark.parametrize(
    "impl, error_msg",
    [
        pytest.param(
            bad_pydough_impl_01,
            "Unrecognized term of simple table collection 'Nations' in graph 'TPCH': 'acctbal'",
            id="01",
        ),
    ],
)
def test_qualify_error(
    impl: Callable[[UnqualifiedNode], UnqualifiedNode],
    error_msg: str,
    get_sample_graph: graph_fetcher,
):
    """
    Tests that strings representing the setup of PyDough unqualified objects
    (with unknown variables already pre-pended with `_ROOT.`) are correctly
    transformed into UnqualifiedNode objects with an expected string
    representation. Each `pydough_str` should be called with `exec` to define
    a variable `answer` that is an `UnqualifiedNode` instance.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = impl(root)
    with pytest.raises(Exception, match=re.escape(error_msg)):
        qualify_node(unqualified, graph)
