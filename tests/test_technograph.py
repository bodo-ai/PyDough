"""
Integration tests for the PyDough workflow with custom questions on the custom
Technograph dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from technograph_pydough_functions import global_incident_rate
from test_utils import graph_fetcher

from pydough import init_pydough_context, to_df, to_sql
from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.metadata import GraphMetadata
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.relational import RelationalRoot
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)


@pytest.fixture(
    params=[
        pytest.param(
            (
                global_incident_rate,
                "global_incident_rate",
                lambda: pd.DataFrame({"irpm": [42.3]}),
            ),
            id="global_incident_rate",
        ),
    ],
)
def pydough_pipeline_test_data_technograph(
    request,
) -> tuple[
    Callable[[], UnqualifiedNode],
    str,
    Callable[[], pd.DataFrame],
]:
    """
    Test data for e2e tests using technograph test data. Returns a tuple of the
    following arguments:
    1. `unqualified_impl`: a function that takes in an unqualified root and
    creates the unqualified node for the TPCH query.
    2. `file_name`: the name of the file containing the expected relational
    plan.
    3. `answer_impl`: a function that takes in nothing and returns the answer
    to a TPCH query as a Pandas DataFrame.
    """
    return request.param


def test_pipeline_until_relational_technograph(
    pydough_pipeline_test_data_technograph: tuple[
        Callable[[], UnqualifiedNode],
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    default_config: PyDoughConfigs,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom epoch dataset
    into relational plans.
    """
    unqualified_impl, file_name, _ = pydough_pipeline_test_data_technograph
    file_path: str = get_plan_test_filename(file_name)
    graph: GraphMetadata = get_sample_graph("TechnoGraph")
    UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph, default_config)
    assert isinstance(qualified, PyDoughCollectionQDAG), (
        "Expected qualified answer to be a collection, not an expression"
    )
    relational: RelationalRoot = convert_ast_to_relational(
        qualified, None, default_config
    )
    if update_tests:
        with open(file_path, "w") as f:
            f.write(relational.to_tree_string() + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert relational.to_tree_string() == expected_relational_string.strip(), (
            "Mismatch between tree string representation of relational node and expected Relational tree string"
        )


def test_pipeline_until_sql_technograph(
    pydough_pipeline_test_data_technograph: tuple[
        Callable[[], UnqualifiedNode],
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    defog_config: PyDoughConfigs,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the PyDough queries on the custom epoch dataset
    into SQL text.
    """
    unqualified_impl, test_name, _ = pydough_pipeline_test_data_technograph
    file_name: str = f"epoch_{test_name}"
    file_path: str = get_sql_test_filename(file_name, empty_context_database.dialect)
    graph: GraphMetadata = get_sample_graph("TechnoGraph")
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    sql_text: str = to_sql(
        unqualified,
        metadata=graph,
        database=empty_context_database,
        config=defog_config,
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
def test_pipeline_e2e_technograph(
    pydough_pipeline_test_data_technograph: tuple[
        Callable[[], UnqualifiedNode],
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    sqlite_technograph_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom epoch dataset against
    the refsol DataFrame.
    """
    unqualified_impl, _, answer_impl = pydough_pipeline_test_data_technograph
    graph: GraphMetadata = get_sample_graph("TechnoGraph")
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(
        root, metadata=graph, database=sqlite_technograph_connection
    )
    pd.testing.assert_frame_equal(result, answer_impl())
