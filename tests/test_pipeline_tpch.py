"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pytest

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.test_pydough_functions.tpch_outputs import (
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
from tests.test_pydough_functions.tpch_test_functions import (
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
from tests.testing_utilities import (
    graph_fetcher,
)

from .testing_utilities import PyDoughPandasTest


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q1,
                "TPCH",
                tpch_q1_output,
                "tpch_q1",
            ),
            id="tpch_q1",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q2,
                "TPCH",
                tpch_q2_output,
                "tpch_q2",
            ),
            id="tpch_q2",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q3,
                "TPCH",
                tpch_q3_output,
                "tpch_q3",
            ),
            id="tpch_q3",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q4,
                "TPCH",
                tpch_q4_output,
                "tpch_q4",
            ),
            id="tpch_q4",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q5,
                "TPCH",
                tpch_q5_output,
                "tpch_q5",
            ),
            id="tpch_q5",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q6,
                "TPCH",
                tpch_q6_output,
                "tpch_q6",
            ),
            id="tpch_q6",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q7,
                "TPCH",
                tpch_q7_output,
                "tpch_q7",
            ),
            id="tpch_q7",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q8,
                "TPCH",
                tpch_q8_output,
                "tpch_q8",
            ),
            id="tpch_q8",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q9,
                "TPCH",
                tpch_q9_output,
                "tpch_q9",
            ),
            id="tpch_q9",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q10,
                "TPCH",
                tpch_q10_output,
                "tpch_q10",
            ),
            id="tpch_q10",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q11,
                "TPCH",
                tpch_q11_output,
                "tpch_q11",
            ),
            id="tpch_q11",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q12,
                "TPCH",
                tpch_q12_output,
                "tpch_q12",
            ),
            id="tpch_q12",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q13,
                "TPCH",
                tpch_q13_output,
                "tpch_q13",
            ),
            id="tpch_q13",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q14,
                "TPCH",
                tpch_q14_output,
                "tpch_q14",
            ),
            id="tpch_q14",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q15,
                "TPCH",
                tpch_q15_output,
                "tpch_q15",
            ),
            id="tpch_q15",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q16,
                "TPCH",
                tpch_q16_output,
                "tpch_q16",
            ),
            id="tpch_q16",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q17,
                "TPCH",
                tpch_q17_output,
                "tpch_q17",
            ),
            id="tpch_q17",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q18,
                "TPCH",
                tpch_q18_output,
                "tpch_q18",
            ),
            id="tpch_q18",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q19,
                "TPCH",
                tpch_q19_output,
                "tpch_q19",
            ),
            id="tpch_q19",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q20,
                "TPCH",
                tpch_q20_output,
                "tpch_q20",
            ),
            id="tpch_q20",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q21,
                "TPCH",
                tpch_q21_output,
                "tpch_q21",
            ),
            id="tpch_q21",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q22,
                "TPCH",
                tpch_q22_output,
                "tpch_q22",
            ),
            id="tpch_q22",
        ),
    ],
)
def pydough_pipeline_tpch_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the 22 TPC-H queries. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


def test_pipeline_until_relational_tpch(
    pydough_pipeline_tpch_test_data: PyDoughPandasTest,
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
    file_path: str = get_plan_test_filename(pydough_pipeline_tpch_test_data.test_name)
    pydough_pipeline_tpch_test_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )


def test_pipeline_until_sql_tpch(
    pydough_pipeline_tpch_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Same as test_pipeline_until_relational_tpch, but for the generated SQL text.
    """
    file_path: str = get_sql_test_filename(
        pydough_pipeline_tpch_test_data.test_name, empty_context_database.dialect
    )
    pydough_pipeline_tpch_test_data.run_sql_test(
        get_sample_graph, file_path, update_tests, empty_context_database
    )


@pytest.mark.execute
def test_pipeline_e2e_tpch(
    pydough_pipeline_tpch_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    pydough_pipeline_tpch_test_data.run_e2e_test(
        get_sample_graph,
        sqlite_tpch_db_context,
    )
