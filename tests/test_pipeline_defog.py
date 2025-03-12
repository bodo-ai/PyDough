"""
Integration tests for the PyDough workflow on the defog.ai queries.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from defog_outputs import (
    defog_sql_text_broker_adv1,
    defog_sql_text_broker_adv2,
    defog_sql_text_broker_adv3,
    defog_sql_text_broker_adv4,
    defog_sql_text_broker_adv5,
    defog_sql_text_broker_adv6,
    defog_sql_text_broker_adv7,
    defog_sql_text_broker_adv8,
    defog_sql_text_broker_adv9,
    defog_sql_text_broker_adv10,
    defog_sql_text_broker_adv11,
    defog_sql_text_broker_adv12,
    defog_sql_text_broker_adv13,
    defog_sql_text_broker_adv14,
    defog_sql_text_broker_adv15,
    defog_sql_text_broker_adv16,
    defog_sql_text_broker_basic1,
    defog_sql_text_broker_basic2,
    defog_sql_text_broker_basic3,
    defog_sql_text_broker_basic4,
    defog_sql_text_broker_basic5,
    defog_sql_text_broker_basic6,
    defog_sql_text_broker_basic7,
    defog_sql_text_broker_basic8,
    defog_sql_text_broker_basic9,
    defog_sql_text_broker_basic10,
    defog_sql_text_broker_gen1,
    defog_sql_text_broker_gen2,
    defog_sql_text_broker_gen3,
    defog_sql_text_broker_gen4,
    defog_sql_text_broker_gen5,
)
from defog_test_functions import (
    impl_defog_broker_adv1,
    impl_defog_broker_adv2,
    impl_defog_broker_adv3,
    impl_defog_broker_adv4,
    impl_defog_broker_adv5,
    impl_defog_broker_adv6,
    impl_defog_broker_adv7,
    impl_defog_broker_adv8,
    impl_defog_broker_adv9,
    impl_defog_broker_adv10,
    impl_defog_broker_adv11,
    impl_defog_broker_adv12,
    impl_defog_broker_adv13,
    impl_defog_broker_adv14,
    impl_defog_broker_adv15,
    impl_defog_broker_adv16,
    impl_defog_broker_basic1,
    impl_defog_broker_basic2,
    impl_defog_broker_basic3,
    impl_defog_broker_basic4,
    impl_defog_broker_basic5,
    impl_defog_broker_basic6,
    impl_defog_broker_basic7,
    impl_defog_broker_basic8,
    impl_defog_broker_basic9,
    impl_defog_broker_basic10,
    impl_defog_broker_gen1,
    impl_defog_broker_gen2,
    impl_defog_broker_gen3,
    impl_defog_broker_gen4,
    impl_defog_broker_gen5,
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
    assert isinstance(graph, GraphMetadata), (
        "Expected to be metadata for a PyDough graph"
    )


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
                impl_defog_broker_adv2,
                "Broker",
                defog_sql_text_broker_adv2,
            ),
            id="broker_adv2",
        ),
        pytest.param(
            (
                impl_defog_broker_adv3,
                "Broker",
                defog_sql_text_broker_adv3,
            ),
            id="broker_adv3",
        ),
        pytest.param(
            (
                impl_defog_broker_adv4,
                "Broker",
                defog_sql_text_broker_adv4,
            ),
            id="broker_adv4",
        ),
        pytest.param(
            (
                impl_defog_broker_adv5,
                "Broker",
                defog_sql_text_broker_adv5,
            ),
            id="broker_adv5",
        ),
        pytest.param(
            (
                impl_defog_broker_adv6,
                "Broker",
                defog_sql_text_broker_adv6,
            ),
            id="broker_adv6",
        ),
        pytest.param(
            (
                impl_defog_broker_adv7,
                "Broker",
                defog_sql_text_broker_adv7,
            ),
            id="broker_adv7",
        ),
        pytest.param(
            (
                impl_defog_broker_adv8,
                "Broker",
                defog_sql_text_broker_adv8,
            ),
            id="broker_adv8",
            marks=pytest.mark.skip(
                "TODO (gh #271): add 'week' support to PyDough DATETIME function"
            ),
        ),
        pytest.param(
            (
                impl_defog_broker_adv9,
                "Broker",
                defog_sql_text_broker_adv9,
            ),
            id="broker_adv9",
            marks=pytest.mark.skip(
                "TODO (gh #271): add 'week' support to PyDough DATETIME function and DAYOFWEEK and WEEKOFYEAR functions"
            ),
        ),
        pytest.param(
            (
                impl_defog_broker_adv10,
                "Broker",
                defog_sql_text_broker_adv10,
            ),
            id="broker_adv10",
        ),
        pytest.param(
            (
                impl_defog_broker_adv11,
                "Broker",
                defog_sql_text_broker_adv11,
            ),
            id="broker_adv11",
        ),
        pytest.param(
            (
                impl_defog_broker_adv12,
                "Broker",
                defog_sql_text_broker_adv12,
            ),
            id="broker_adv12",
        ),
        pytest.param(
            (
                impl_defog_broker_adv13,
                "Broker",
                defog_sql_text_broker_adv13,
            ),
            id="broker_adv13",
        ),
        pytest.param(
            (
                impl_defog_broker_adv14,
                "Broker",
                defog_sql_text_broker_adv14,
            ),
            id="broker_adv14",
        ),
        pytest.param(
            (
                impl_defog_broker_adv15,
                "Broker",
                defog_sql_text_broker_adv15,
            ),
            id="broker_adv15",
        ),
        pytest.param(
            (
                impl_defog_broker_adv16,
                "Broker",
                defog_sql_text_broker_adv16,
            ),
            id="broker_adv16",
        ),
        pytest.param(
            (
                impl_defog_broker_basic1,
                "Broker",
                defog_sql_text_broker_basic1,
            ),
            id="broker_basic1",
        ),
        pytest.param(
            (
                impl_defog_broker_basic2,
                "Broker",
                defog_sql_text_broker_basic2,
            ),
            id="broker_basic2",
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
        pytest.param(
            (
                impl_defog_broker_basic5,
                "Broker",
                defog_sql_text_broker_basic5,
            ),
            id="broker_basic5",
        ),
        pytest.param(
            (
                impl_defog_broker_basic6,
                "Broker",
                defog_sql_text_broker_basic6,
            ),
            id="broker_basic6",
        ),
        pytest.param(
            (
                impl_defog_broker_basic7,
                "Broker",
                defog_sql_text_broker_basic7,
            ),
            id="broker_basic7",
        ),
        pytest.param(
            (
                impl_defog_broker_basic8,
                "Broker",
                defog_sql_text_broker_basic8,
            ),
            id="broker_basic8",
        ),
        pytest.param(
            (
                impl_defog_broker_basic9,
                "Broker",
                defog_sql_text_broker_basic9,
            ),
            id="broker_basic9",
        ),
        pytest.param(
            (
                impl_defog_broker_basic10,
                "Broker",
                defog_sql_text_broker_basic10,
            ),
            id="broker_basic10",
        ),
        pytest.param(
            (
                impl_defog_broker_gen1,
                "Broker",
                defog_sql_text_broker_gen1,
            ),
            id="broker_gen1",
        ),
        pytest.param(
            (
                impl_defog_broker_gen2,
                "Broker",
                defog_sql_text_broker_gen2,
            ),
            id="broker_gen2",
        ),
        pytest.param(
            (
                impl_defog_broker_gen3,
                "Broker",
                defog_sql_text_broker_gen3,
            ),
            id="broker_gen3",
        ),
        pytest.param(
            (
                impl_defog_broker_gen4,
                "Broker",
                defog_sql_text_broker_gen4,
            ),
            id="broker_gen4",
        ),
        pytest.param(
            (
                impl_defog_broker_gen5,
                "Broker",
                defog_sql_text_broker_gen5,
            ),
            id="broker_gen5",  
        ),
    ],
)
def defog_test_data(
    request,
) -> tuple[Callable[[], UnqualifiedNode], Callable[[], str]]:
    """
    Test data for `test_defog_e2e`. Returns a tuple of the following
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
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector. Run on the defog.ai queries.
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
