"""
Test that tests the full conversion of a PyDough object to a SQL query.
"""

from collections.abc import Callable

from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.relational.relational_nodes.relational_root import RelationalRoot
from pydough.unqualified.qualification import qualify_node
import pytest
from simple_pydough_functions import hour_minute_day, rank_a, rank_b, rank_c, simple_filter, simple_scan
from test_utils import (
    graph_fetcher,
)

from pydough import init_pydough_context, to_sql
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)
from tests.conftest import default_config


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
            "SELECT o_orderkey, o_totalprice FROM (SELECT o_orderkey AS o_orderkey, o_totalprice AS o_totalprice FROM tpch.ORDERS) WHERE o_totalprice < 1000.0",
            id="simple_filter",
        ),
        pytest.param(
            rank_a,
            "SELECT ROW_NUMBER() OVER (ORDER BY acctbal DESC NULLS FIRST) AS rank FROM (SELECT c_acctbal AS acctbal FROM tpch.CUSTOMER)",
            id="rank_a",
        ),
        pytest.param(
            rank_b,
            " SELECT RANK() OVER (ORDER BY order_priority NULLS LAST) AS rank FROM (SELECT o_orderpriority AS order_priority FROM tpch.ORDERS)",
            id="rank_b",
        ),
        pytest.param(
            rank_c,
            "SELECT order_date, DENSE_RANK() OVER (ORDER BY order_date NULLS LAST) AS rank FROM (SELECT o_orderdate AS order_date FROM tpch.ORDERS)",
            id="rank_c",
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
    qualified DAG version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(pydough_code)()
    actual_sql: str = to_sql(root, metadata=graph).strip()
    expected_sql = expected_sql.strip()
    assert actual_sql == expected_sql
