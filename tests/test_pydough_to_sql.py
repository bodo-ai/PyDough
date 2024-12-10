"""
Test that tests the fully conversion of a PyDough object to a SQL query.
"""

from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)

from pydough import init_pydough_context, to_sql
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def simple_scan():
    return Orders(key)


def simple_filter():
    # Note: The SQL is non-deterministic once we add nested expressions.
    return Orders(o_orderkey=key, o_totalprice=total_price).WHERE(o_totalprice < 10.0)


@pytest.mark.parametrize(
    "pydough_code, expected_sql",
    # Note: All of these tests are for simple code because the
    # exact SQL generation for inner expressions is currently
    # non-deterministic.
    [
        pytest.param(
            simple_scan,
            "SELECT o_orderkey AS key FROM tpch.ORDERS",
            id="simple_scan",
        ),
        pytest.param(
            simple_filter,
            "SELECT o_orderkey, o_totalprice FROM (SELECT o_orderkey AS o_orderkey, o_totalprice AS o_totalprice FROM tpch.ORDERS) WHERE o_totalprice < 10.0",
            id="simple_filter",
        ),
    ],
)
def test_pydough_to_sql(
    pydough_code: Callable[[], UnqualifiedNode],
    expected_sql: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified AST version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    t1 = init_pydough_context(graph)
    t2 = t1(pydough_code)
    root: UnqualifiedNode = t2()
    actual_sql: str = to_sql(root, metadata=graph).strip()
    expected_sql = expected_sql.strip()
    assert actual_sql == expected_sql
