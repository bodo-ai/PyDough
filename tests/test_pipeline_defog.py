"""
Integration tests for the PyDough workflow on the defog.ai queries.
"""

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
    defog_sql_text_ewallet_adv1,
    defog_sql_text_ewallet_adv2,
    defog_sql_text_ewallet_adv3,
    defog_sql_text_ewallet_adv4,
    defog_sql_text_ewallet_adv5,
    defog_sql_text_ewallet_adv6,
    defog_sql_text_ewallet_adv7,
    defog_sql_text_ewallet_adv8,
    defog_sql_text_ewallet_adv9,
    defog_sql_text_ewallet_adv10,
    defog_sql_text_ewallet_adv11,
    defog_sql_text_ewallet_adv12,
    defog_sql_text_ewallet_adv13,
    defog_sql_text_ewallet_adv14,
    defog_sql_text_ewallet_adv15,
    defog_sql_text_ewallet_adv16,
    defog_sql_text_ewallet_basic1,
    defog_sql_text_ewallet_basic2,
    defog_sql_text_ewallet_basic3,
    defog_sql_text_ewallet_basic4,
    defog_sql_text_ewallet_basic5,
    defog_sql_text_ewallet_basic6,
    defog_sql_text_ewallet_basic7,
    defog_sql_text_ewallet_basic8,
    defog_sql_text_ewallet_basic9,
    defog_sql_text_ewallet_basic10,
    defog_sql_text_ewallet_gen1,
    defog_sql_text_ewallet_gen2,
    defog_sql_text_ewallet_gen3,
    defog_sql_text_ewallet_gen4,
    defog_sql_text_ewallet_gen5,
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
    impl_defog_ewallet_adv1,
    impl_defog_ewallet_adv2,
    impl_defog_ewallet_adv3,
    impl_defog_ewallet_adv4,
    impl_defog_ewallet_adv5,
    impl_defog_ewallet_adv6,
    impl_defog_ewallet_adv7,
    impl_defog_ewallet_adv8,
    impl_defog_ewallet_adv9,
    impl_defog_ewallet_adv10,
    impl_defog_ewallet_adv11,
    impl_defog_ewallet_adv12,
    impl_defog_ewallet_adv13,
    impl_defog_ewallet_adv14,
    impl_defog_ewallet_adv15,
    impl_defog_ewallet_adv16,
    impl_defog_ewallet_basic1,
    impl_defog_ewallet_basic2,
    impl_defog_ewallet_basic3,
    impl_defog_ewallet_basic4,
    impl_defog_ewallet_basic5,
    impl_defog_ewallet_basic6,
    impl_defog_ewallet_basic7,
    impl_defog_ewallet_basic8,
    impl_defog_ewallet_basic9,
    impl_defog_ewallet_basic10,
    impl_defog_ewallet_gen1,
    impl_defog_ewallet_gen2,
    impl_defog_ewallet_gen3,
    impl_defog_ewallet_gen4,
    impl_defog_ewallet_gen5,
)
from test_utils import (
    PyDoughSQLComparisonTest,
    graph_fetcher,
)

from pydough import init_pydough_context, to_df, to_sql
from pydough.database_connectors import DatabaseContext, DatabaseDialect
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
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv1,
                "Broker",
                defog_sql_text_broker_adv1,
            ),
            id="broker_adv1",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv2,
                "Broker",
                defog_sql_text_broker_adv2,
            ),
            id="broker_adv2",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv3,
                "Broker",
                defog_sql_text_broker_adv3,
            ),
            id="broker_adv3",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv4,
                "Broker",
                defog_sql_text_broker_adv4,
            ),
            id="broker_adv4",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv5,
                "Broker",
                defog_sql_text_broker_adv5,
                order_insensitive=True,
            ),
            id="broker_adv5",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv6,
                "Broker",
                defog_sql_text_broker_adv6,
            ),
            id="broker_adv6",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv7,
                "Broker",
                defog_sql_text_broker_adv7,
            ),
            id="broker_adv7",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
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
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv9,
                "Broker",
                defog_sql_text_broker_adv9,
            ),
            id="broker_adv9",
            marks=pytest.mark.skip(
                "TODO (gh #271): add 'week' support to PyDough DATETIME function and DAYOFWEEK functions"
            ),
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv10,
                "Broker",
                defog_sql_text_broker_adv10,
            ),
            id="broker_adv10",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv11,
                "Broker",
                defog_sql_text_broker_adv11,
            ),
            id="broker_adv11",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv12,
                "Broker",
                defog_sql_text_broker_adv12,
            ),
            id="broker_adv12",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv13,
                "Broker",
                defog_sql_text_broker_adv13,
            ),
            id="broker_adv13",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv14,
                "Broker",
                defog_sql_text_broker_adv14,
            ),
            id="broker_adv14",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv15,
                "Broker",
                defog_sql_text_broker_adv15,
            ),
            id="broker_adv15",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_adv16,
                "Broker",
                defog_sql_text_broker_adv16,
            ),
            id="broker_adv16",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic1,
                "Broker",
                defog_sql_text_broker_basic1,
            ),
            id="broker_basic1",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic2,
                "Broker",
                defog_sql_text_broker_basic2,
            ),
            id="broker_basic2",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic3,
                "Broker",
                defog_sql_text_broker_basic3,
            ),
            id="broker_basic3",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic4,
                "Broker",
                defog_sql_text_broker_basic4,
            ),
            id="broker_basic4",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic5,
                "Broker",
                defog_sql_text_broker_basic5,
                order_insensitive=True,
            ),
            id="broker_basic5",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic6,
                "Broker",
                defog_sql_text_broker_basic6,
            ),
            id="broker_basic6",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic7,
                "Broker",
                defog_sql_text_broker_basic7,
            ),
            id="broker_basic7",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic8,
                "Broker",
                defog_sql_text_broker_basic8,
            ),
            id="broker_basic8",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic9,
                "Broker",
                defog_sql_text_broker_basic9,
            ),
            id="broker_basic9",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_basic10,
                "Broker",
                defog_sql_text_broker_basic10,
            ),
            id="broker_basic10",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_gen1,
                "Broker",
                defog_sql_text_broker_gen1,
            ),
            id="broker_gen1",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_gen2,
                "Broker",
                defog_sql_text_broker_gen2,
            ),
            id="broker_gen2",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_gen3,
                "Broker",
                defog_sql_text_broker_gen3,
            ),
            id="broker_gen3",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_gen4,
                "Broker",
                defog_sql_text_broker_gen4,
            ),
            id="broker_gen4",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_broker_gen5,
                "Broker",
                defog_sql_text_broker_gen5,
            ),
            id="broker_gen5",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv1,
                "Ewallet",
                defog_sql_text_ewallet_adv1,
                order_insensitive=True,
            ),
            id="ewallet_adv1",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv2,
                "Ewallet",
                defog_sql_text_ewallet_adv2,
            ),
            id="ewallet_adv2",
            marks=pytest.mark.skip(
                "TODO (gh #271): add 'week' support to PyDough DATETIME function and DAYOFWEEK functions"
            ),
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv3,
                "Ewallet",
                defog_sql_text_ewallet_adv3,
                order_insensitive=True,
            ),
            id="ewallet_adv3",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv4,
                "Ewallet",
                defog_sql_text_ewallet_adv4,
            ),
            id="ewallet_adv4",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv5,
                "Ewallet",
                defog_sql_text_ewallet_adv5,
            ),
            id="ewallet_adv5",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv6,
                "Ewallet",
                defog_sql_text_ewallet_adv6,
            ),
            id="ewallet_adv6",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv7,
                "Ewallet",
                defog_sql_text_ewallet_adv7,
            ),
            id="ewallet_adv7",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv8,
                "Ewallet",
                defog_sql_text_ewallet_adv8,
            ),
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv9,
                "Ewallet",
                defog_sql_text_ewallet_adv9,
            ),
            id="ewallet_adv9",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv10,
                "Ewallet",
                defog_sql_text_ewallet_adv10,
            ),
            id="ewallet_adv10",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv11,
                "Ewallet",
                defog_sql_text_ewallet_adv11,
            ),
            id="ewallet_adv11",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv12,
                "Ewallet",
                defog_sql_text_ewallet_adv12,
            ),
            id="ewallet_adv12",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv13,
                "Ewallet",
                defog_sql_text_ewallet_adv13,
            ),
            id="ewallet_adv13",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv14,
                "Ewallet",
                defog_sql_text_ewallet_adv14,
            ),
            id="ewallet_adv14",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv15,
                "Ewallet",
                defog_sql_text_ewallet_adv15,
            ),
            id="ewallet_adv15",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_adv16,
                "Ewallet",
                defog_sql_text_ewallet_adv16,
            ),
            id="ewallet_adv16",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic1,
                "Ewallet",
                defog_sql_text_ewallet_basic1,
            ),
            id="ewallet_basic1",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic10,
                "Ewallet",
                defog_sql_text_ewallet_basic10,
            ),
            id="ewallet_basic10",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic2,
                "Ewallet",
                defog_sql_text_ewallet_basic2,
            ),
            id="ewallet_basic2",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic3,
                "Ewallet",
                defog_sql_text_ewallet_basic3,
                order_insensitive=True,
            ),
            id="ewallet_basic3",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic4,
                "Ewallet",
                defog_sql_text_ewallet_basic4,
            ),
            id="ewallet_basic4",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic5,
                "Ewallet",
                defog_sql_text_ewallet_basic5,
            ),
            id="ewallet_basic5",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic6,
                "Ewallet",
                defog_sql_text_ewallet_basic6,
            ),
            id="ewallet_basic6",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic7,
                "Ewallet",
                defog_sql_text_ewallet_basic7,
            ),
            id="ewallet_basic7",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic8,
                "Ewallet",
                defog_sql_text_ewallet_basic8,
                order_insensitive=True,
            ),
            id="ewallet_basic8",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_basic9,
                "Ewallet",
                defog_sql_text_ewallet_basic9,
            ),
            id="ewallet_basic9",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_gen1,
                "Ewallet",
                defog_sql_text_ewallet_gen1,
            ),
            id="ewallet_gen1",
            marks=pytest.mark.skip(
                "TODO (gh #305): #305 Add support for MEDIAN aggregation function"
            ),
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_gen2,
                "Ewallet",
                defog_sql_text_ewallet_gen2,
            ),
            id="ewallet_gen2",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_gen3,
                "Ewallet",
                defog_sql_text_ewallet_gen3,
            ),
            id="ewallet_gen3",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_gen4,
                "Ewallet",
                defog_sql_text_ewallet_gen4,
            ),
            id="ewallet_gen4",
        ),
        pytest.param(
            PyDoughSQLComparisonTest(
                impl_defog_ewallet_gen5,
                "Ewallet",
                defog_sql_text_ewallet_gen5,
            ),
            id="ewallet_gen5",
        ),
    ],
)
def defog_test_data(
    request,
) -> PyDoughSQLComparisonTest:
    """
    Returns a dataclass encapsulating all of the information needed to run the
    PyDough code and compare the result against a refsol derived via SQL.
    """
    return request.param


def test_defog_until_sql(
    defog_test_data: tuple[Callable[[], UnqualifiedNode], str, Callable[[], str]],
    defog_graphs: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the defog analytical questions to SQL.
    """
    unqualified_impl, graph_name, _ = defog_test_data
    test_name: str = unqualified_impl.__name__.split("_")[-1]
    file_name: str = f"defog_{graph_name.lower()}_{test_name}"
    file_path: str = get_sql_test_filename(file_name, empty_context_database.dialect)
    graph: GraphMetadata = defog_graphs(graph_name)
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    sql_text: str = to_sql(
        unqualified,
        metadata=graph,
        database=empty_context_database,
    )
    if update_tests:
        with open(file_path, "w") as f:
            f.write(sql_text + "\n")
    else:
        with open(file_path) as f:
            expected_sql_text: str = f.read()
        assert sql_text == expected_sql_text.strip(), (
            "Mismatch between SQL text produced expected SQL text"
        )


@pytest.mark.execute
def test_defog_e2e(
    defog_test_data: PyDoughSQLComparisonTest,
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
) -> None:
    """
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector. Run on the defog.ai queries.
    """
    graph: GraphMetadata = defog_graphs(defog_test_data.graph_name)
    root: UnqualifiedNode = init_pydough_context(graph)(
        defog_test_data.pydough_function
    )()
    result: pd.DataFrame = to_df(root, metadata=graph, database=sqlite_defog_connection)
    sqlite_query: str = defog_test_data.sql_function()
    refsol: pd.DataFrame = sqlite_defog_connection.connection.execute_query_df(
        sqlite_query
    )
    assert len(result.columns) == len(refsol.columns)
    refsol.columns = result.columns
    # If the query is order-insensitive, sort the DataFrames before comparison
    if defog_test_data.order_insensitive:
        result = result.sort_values(by=list(refsol.columns)).reset_index(drop=True)
        refsol = refsol.sort_values(by=list(refsol.columns)).reset_index(drop=True)
    pd.testing.assert_frame_equal(result, refsol)
