"""
Test that tests the full conversion of a PyDough object to evaluating
SQL and returning a DataFrame.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from simple_pydough_functions import simple_filter_top_five, simple_scan_top_five
from test_utils import graph_fetcher

from pydough import init_pydough_context, to_df
from pydough.database_connectors import DatabaseContext
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode

pytestmark = [pytest.mark.execute]


@pytest.mark.parametrize(
    "pydough_code, expected_df",
    [
        pytest.param(
            simple_scan_top_five,
            pd.DataFrame(
                {
                    "key": [1, 2, 3, 4, 5],
                }
            ),
            id="simple_scan_top_five",
        ),
        pytest.param(
            simple_filter_top_five,
            pd.DataFrame(
                {
                    "key": [5989315, 5935174, 5881093, 5876066, 5866437],
                    "total_price": [947.81, 974.01, 995.6, 967.55, 916.41],
                }
            ),
            id="simple_filter_top_five",
        ),
    ],
)
def test_pydough_to_df(
    pydough_code: Callable[[], UnqualifiedNode],
    expected_df: pd.DataFrame,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified AST version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(pydough_code)()
    result: pd.DataFrame = to_df(root, metadata=graph, database=sqlite_tpch_db_context)
    pd.testing.assert_frame_equal(result, expected_df)
