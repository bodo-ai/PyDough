"""
Test that tests the fully conversion of a PyDough object to a SQL query.
"""

from collections.abc import Callable

import pytest
from test_qualification import (
    pydough_impl_tpch_q1,
)
from test_utils import (
    graph_fetcher,
)

from pydough import to_sql
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
)


@pytest.mark.parametrize(
    "pydough_code, expected_sql",
    [
        pytest.param(
            pydough_impl_tpch_q1,
            """""",
            id="tpch_q1",
        )
    ],
)
def test_pydough_to_sql(
    pydough_code: Callable[[UnqualifiedNode], UnqualifiedNode],
    expected_sql: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified AST version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = pydough_code(root)
    actual_sql: str = to_sql(unqualified, metadata=graph)
    assert actual_sql == expected_sql
