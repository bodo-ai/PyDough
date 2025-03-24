"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from test_utils import (
    graph_fetcher,
)
from tpch_outputs import (
    tpch_q1_output,
    tpch_q2_output,
    tpch_q3_output,
    tpch_q4_output,
    tpch_q5_output,
    tpch_q6_output,
    tpch_q7_output,
    tpch_q8_output,
    tpch_q9_output,
    tpch_q10_output,
    tpch_q11_output,
    tpch_q12_output,
    tpch_q13_output,
    tpch_q14_output,
    tpch_q15_output,
    tpch_q16_output,
    tpch_q17_output,
    tpch_q18_output,
    tpch_q19_output,
    tpch_q20_output,
    tpch_q21_output,
    tpch_q22_output,
)
from tpch_test_functions import (
    impl_tpch_q1,
    impl_tpch_q2,
    impl_tpch_q3,
    impl_tpch_q4,
    impl_tpch_q5,
    impl_tpch_q6,
    impl_tpch_q7,
    impl_tpch_q8,
    impl_tpch_q9,
    impl_tpch_q10,
    impl_tpch_q11,
    impl_tpch_q12,
    impl_tpch_q13,
    impl_tpch_q14,
    impl_tpch_q15,
    impl_tpch_q16,
    impl_tpch_q17,
    impl_tpch_q18,
    impl_tpch_q19,
    impl_tpch_q20,
    impl_tpch_q21,
    impl_tpch_q22,
)

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
                impl_tpch_q1,
                "tpch_q1",
                tpch_q1_output,
            ),
            id="tpch_q1",
        ),
        pytest.param(
            (
                impl_tpch_q2,
                "tpch_q2",
                tpch_q2_output,
            ),
            id="tpch_q2",
        ),
        pytest.param(
            (
                impl_tpch_q3,
                "tpch_q3",
                tpch_q3_output,
            ),
            id="tpch_q3",
        ),
        pytest.param(
            (
                impl_tpch_q4,
                "tpch_q4",
                tpch_q4_output,
            ),
            id="tpch_q4",
        ),
        pytest.param(
            (
                impl_tpch_q5,
                "tpch_q5",
                tpch_q5_output,
            ),
            id="tpch_q5",
        ),
        pytest.param(
            (
                impl_tpch_q6,
                "tpch_q6",
                tpch_q6_output,
            ),
            id="tpch_q6",
        ),
        pytest.param(
            (
                impl_tpch_q7,
                "tpch_q7",
                tpch_q7_output,
            ),
            id="tpch_q7",
        ),
        pytest.param(
            (
                impl_tpch_q8,
                "tpch_q8",
                tpch_q8_output,
            ),
            id="tpch_q8",
        ),
        pytest.param(
            (
                impl_tpch_q9,
                "tpch_q9",
                tpch_q9_output,
            ),
            id="tpch_q9",
        ),
        pytest.param(
            (
                impl_tpch_q10,
                "tpch_q10",
                tpch_q10_output,
            ),
            id="tpch_q10",
        ),
        pytest.param(
            (
                impl_tpch_q11,
                "tpch_q11",
                tpch_q11_output,
            ),
            id="tpch_q11",
        ),
        pytest.param(
            (
                impl_tpch_q12,
                "tpch_q12",
                tpch_q12_output,
            ),
            id="tpch_q12",
        ),
        pytest.param(
            (
                impl_tpch_q13,
                "tpch_q13",
                tpch_q13_output,
            ),
            id="tpch_q13",
        ),
        pytest.param(
            (
                impl_tpch_q14,
                "tpch_q14",
                tpch_q14_output,
            ),
            id="tpch_q14",
        ),
        pytest.param(
            (
                impl_tpch_q15,
                "tpch_q15",
                tpch_q15_output,
            ),
            id="tpch_q15",
        ),
        pytest.param(
            (
                impl_tpch_q16,
                "tpch_q16",
                tpch_q16_output,
            ),
            id="tpch_q16",
        ),
        pytest.param(
            (
                impl_tpch_q17,
                "tpch_q17",
                tpch_q17_output,
            ),
            id="tpch_q17",
        ),
        pytest.param(
            (
                impl_tpch_q18,
                "tpch_q18",
                tpch_q18_output,
            ),
            id="tpch_q18",
        ),
        pytest.param(
            (
                impl_tpch_q19,
                "tpch_q19",
                tpch_q19_output,
            ),
            id="tpch_q19",
        ),
        pytest.param(
            (
                impl_tpch_q20,
                "tpch_q20",
                tpch_q20_output,
            ),
            id="tpch_q20",
        ),
        pytest.param(
            (
                impl_tpch_q21,
                "tpch_q21",
                tpch_q21_output,
            ),
            id="tpch_q21",
        ),
        pytest.param(
            (
                impl_tpch_q22,
                "tpch_q22",
                tpch_q22_output,
            ),
            id="tpch_q22",
        ),
    ],
)
def pydough_pipeline_tpch_test_data(
    request,
) -> tuple[
    Callable[[], UnqualifiedNode],
    str,
    Callable[[], pd.DataFrame],
]:
    """
    Test data for test_pydough_pipeline. Returns a tuple of the following
    arguments:
    1. `unqualified_impl`: a function that takes in an unqualified root and
    creates the unqualified node for the TPCH query.
    2. `file_name`: the name of the file containing the expected relational
    plan.
    3. `answer_impl`: a function that takes in nothing and returns the answer
    to a TPCH query as a Pandas DataFrame.
    """
    return request.param


def test_pipeline_until_relational_tpch(
    pydough_pipeline_tpch_test_data: tuple[
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
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on the
    22 TPC-H queries.
    """
    # Run the query through the stages from unqualified node to qualified node
    # to relational tree, and confirm the tree string matches the expected
    # structure.
    unqualified_impl, file_name, _ = pydough_pipeline_tpch_test_data
    file_path: str = get_plan_test_filename(file_name)
    graph: GraphMetadata = get_sample_graph("TPCH")
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


def test_pipeline_until_sql_tpch(
    pydough_pipeline_tpch_test_data: tuple[
        Callable[[], UnqualifiedNode],
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    default_config: PyDoughConfigs,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Same as test_pipeline_until_relational_tpch, but for the generated SQL text.
    """
    # Run the query through the stages from unqualified node to qualified node
    # to relational tree, and confirm the tree string matches the expected
    # structure.
    unqualified_impl, file_name, _ = pydough_pipeline_tpch_test_data
    file_path: str = get_sql_test_filename(file_name, empty_context_database.dialect)
    graph: GraphMetadata = get_sample_graph("TPCH")
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    sql_text: str = to_sql(
        unqualified,
        metadata=graph,
        database=empty_context_database,
        config=default_config,
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
def test_pipeline_e2e_tpch(
    pydough_pipeline_tpch_test_data: tuple[
        Callable[[], UnqualifiedNode],
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
    default_config: PyDoughConfigs,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    unqualified_impl, _, answer_impl = pydough_pipeline_tpch_test_data
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(
        root, metadata=graph, database=sqlite_tpch_db_context, config=default_config
    )
    pd.testing.assert_frame_equal(result, answer_impl())
