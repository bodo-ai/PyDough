"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from defog_outputs import (
    defog_sql_text_broker_adv1,
    defog_sql_text_broker_basic3,
    defog_sql_text_broker_basic4,
)
from defog_test_functions import (
    impl_defog_broker_adv1,
    impl_defog_broker_basic3,
    impl_defog_broker_basic4,
)
from test_utils import (
    graph_fetcher,
)

from pydough import init_pydough_context, to_df
from pydough.database_connectors import DatabaseContext
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)


@pytest.mark.parametrize(
    "graph_name",
    [
        "Broker",
        "Dealership",
        "DermTreatment",
        "Ewallet",
    ],
)
def test_graph_structure_defog(defog_graphs: graph_fetcher, graph_name: str) -> None:
    """
    Testing that the metadata for the defog graphs is parsed correctly.
    """
    graph = defog_graphs(graph_name)
    assert isinstance(
        graph, GraphMetadata
    ), "Expected to be metadata for a PyDough graph"


@pytest.fixture(
    params=[
        pytest.param(
            (
                impl_defog_broker_adv1,
                "Broker",
                defog_sql_text_broker_adv1,
            ),
            id="broker_adv1",
        ),
        pytest.param(
            (
                impl_defog_broker_basic3,
                "Broker",
                defog_sql_text_broker_basic3,
            ),
            id="broker_basic3",
        ),
        pytest.param(
            (
                impl_defog_broker_basic4,
                "Broker",
                defog_sql_text_broker_basic4,
            ),
            id="broker_basic4",
        ),
    ],
)
def defog_test_data(
    request,
) -> tuple[Callable[[], UnqualifiedNode], Callable[[], str]]:
    """
    Test data for test_defog_e2e. Returns a tuple of the following
    arguments:
    1. `unqualified_impl`: a PyDough implementation function.
    2. `graph_name`: the name of the graph from the defog database to use.
    3. `query`: a function that takes in nothing and returns the sqlite query
    text for a defog query.
    """
    return request.param


@pytest.mark.execute
def test_defog_e2e(
    defog_test_data: tuple[Callable[[], UnqualifiedNode], str, Callable[[], str]],
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    unqualified_impl, graph_name, query_impl = defog_test_data
    graph: GraphMetadata = defog_graphs(graph_name)
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(root, metadata=graph, database=sqlite_defog_connection)
    sqlite_query: str = query_impl()
    refsol: pd.DataFrame = sqlite_defog_connection.connection.execute_query_df(
        sqlite_query
    )
    assert len(result.columns) == len(refsol.columns)
    refsol.columns = result.columns
    pd.testing.assert_frame_equal(result, refsol)
