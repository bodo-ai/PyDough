"""
Integration tests for the PyDough workflow with custom questions on the TPC-H
dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.test_pydough_functions.simple_pydough_functions import pagerank

from .testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_A",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4],
                        "page_rank": [0.25] * 4,
                    }
                ),
                "pagerank_a0",
                order_sensitive=True,
                args=[0],
            ),
            id="pagerank_a0",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_A",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4],
                        "page_rank": [0.25, 0.35625, 0.14375, 0.25],
                    }
                ),
                "pagerank_a1",
                order_sensitive=True,
                args=[1],
            ),
            id="pagerank_a1",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_A",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4],
                        "page_rank": [0.29516, 0.35625, 0.18891, 0.15969],
                    }
                ),
                "pagerank_a2",
                order_sensitive=True,
                args=[2],
            ),
            id="pagerank_a2",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_A",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4],
                        "page_rank": [0.27205, 0.34791, 0.18787, 0.19218],
                    }
                ),
                "pagerank_a6",
                order_sensitive=True,
                args=[6],
            ),
            id="pagerank_a6",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_B",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4, 5],
                        "page_rank": [0.2] * 5,
                    }
                ),
                "pagerank_b0",
                skip_relational=True,
                skip_sql=True,
                order_sensitive=True,
                args=[0],
            ),
            id="pagerank_b0",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_B",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4, 5],
                        "page_rank": [0.115, 0.455, 0.2, 0.03, 0.2],
                    }
                ),
                "pagerank_b1",
                skip_relational=True,
                skip_sql=True,
                order_sensitive=True,
                args=[1],
            ),
            id="pagerank_b1",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_B",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4, 5],
                        "page_rank": [0.16196, 0.40262, 0.23071, 0.03, 0.17471],
                    }
                ),
                "pagerank_b3",
                order_sensitive=True,
                args=[3],
            ),
            id="pagerank_b3",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_C",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4, 5, 6, 7, 8],
                        "page_rank": [
                            0.08996,
                            0.19353,
                            0.11764,
                            0.03252,
                            0.10377,
                            0.12682,
                            0.16788,
                            0.16788,
                        ],
                    }
                ),
                "pagerank_c4",
                order_sensitive=True,
                args=[4],
            ),
            id="pagerank_c4",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_D",
                lambda: pd.DataFrame(
                    {
                        "key": range(1, 17),
                        "page_rank": [
                            0.0459,
                            0.18314,
                            0.0459,
                            0.05918,
                            0.10345,
                            0.0459,
                            0.09902,
                            0.07246,
                            0.01934,
                            0.0459,
                            0.05033,
                            0.07246,
                            0.0459,
                            0.05918,
                            0.01934,
                            0.03262,
                        ],
                    }
                ),
                "pagerank_d1",
                skip_relational=True,
                skip_sql=True,
                order_sensitive=True,
                args=[1],
            ),
            id="pagerank_d1",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_D",
                lambda: pd.DataFrame(
                    {
                        "key": range(1, 17),
                        "page_rank": [
                            0.06896,
                            0.11157,
                            0.05385,
                            0.04884,
                            0.11486,
                            0.05966,
                            0.10651,
                            0.10618,
                            0.01647,
                            0.02365,
                            0.05369,
                            0.06529,
                            0.04508,
                            0.06876,
                            0.01647,
                            0.04015,
                        ],
                    }
                ),
                "pagerank_d5",
                order_sensitive=True,
                args=[5],
            ),
            id="pagerank_d5",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_E",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4, 5],
                        "page_rank": [0.2] * 5,
                    }
                ),
                "pagerank_e1",
                skip_relational=True,
                skip_sql=True,
                order_sensitive=True,
                args=[1],
            ),
            id="pagerank_e1",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_F",
                lambda: pd.DataFrame(
                    {
                        "key": list(range(1, 101)),
                        "page_rank": [0.01] * 100,
                    }
                ),
                "pagerank_f2",
                skip_relational=True,
                skip_sql=True,
                order_sensitive=True,
                args=[2],
            ),
            id="pagerank_f2",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_G",
                lambda: pd.DataFrame(
                    {
                        "key": list(range(1, 1001)),
                        "page_rank": [0.02471]
                        + [0.03806] * 9
                        + [
                            0.00225
                            if i % 10 == 0
                            else (0.00234 if len(set(str(i))) == 1 else 0.00205)
                            for i in range(10, 100)
                        ]
                        + [0.00049] * 900,
                    }
                ),
                "pagerank_g5",
                skip_relational=True,
                skip_sql=True,
                order_sensitive=True,
                args=[5],
            ),
            id="pagerank_g5",
        ),
        pytest.param(
            PyDoughPandasTest(
                pagerank,
                "PAGERANK_H",
                lambda: pd.DataFrame(
                    {
                        "key": list(range(1, 51)),
                        "page_rank": [
                            0.07399,
                            0.03496,
                            0.0178,
                            0.01513,
                            0.02271,
                            0.01277,
                            0.02365,
                            0.01234,
                            0.01065,
                            0.01941,
                            0.01044,
                            0.01207,
                            0.00915,
                            0.00958,
                            0.01116,
                            0.01242,
                            0.00911,
                            0.00929,
                            0.00993,
                            0.01019,
                            0.01077,
                            0.01195,
                            0.01426,
                            0.01403,
                            0.02148,
                            0.01121,
                            0.01154,
                            0.0119,
                            0.01227,
                            0.01273,
                            0.01313,
                            0.01369,
                            0.0142,
                            0.01482,
                            0.01551,
                            0.01628,
                            0.01694,
                            0.01804,
                            0.01914,
                            0.0204,
                            0.02152,
                            0.02352,
                            0.02499,
                            0.02791,
                            0.03029,
                            0.0331,
                            0.03748,
                            0.04604,
                            0.04977,
                            0.06437,
                        ],
                    }
                ),
                "pagerank_h3",
                skip_relational=True,
                skip_sql=True,
                order_sensitive=True,
                args=[3],
            ),
            id="pagerank_h3",
        ),
    ],
)
def pagerank_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests on custom queries using the TPC-H database and
    sqlite UDFs. Returns an instance of PyDoughPandasTest containing
    information about the test.
    """
    return request.param


def test_pipeline_until_relational_pagerank(
    pagerank_pipeline_test_data: PyDoughPandasTest,
    get_pagerank_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Verifies the generated relational plans for the pagerank tests.
    """
    file_path: str = get_plan_test_filename(pagerank_pipeline_test_data.test_name)
    pagerank_pipeline_test_data.run_relational_test(
        get_pagerank_graph, file_path, update_tests
    )


def test_pipeline_until_sql_pagerank(
    pagerank_pipeline_test_data: PyDoughPandasTest,
    get_pagerank_graph: graph_fetcher,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    sqlite_pagerank_db_contexts: dict[str, DatabaseContext],
    update_tests: bool,
) -> None:
    """
    Verifies the generated SQL for the pagerank tests.
    """
    ctx: DatabaseContext = sqlite_pagerank_db_contexts[
        pagerank_pipeline_test_data.graph_name
    ]
    file_path: str = get_sql_test_filename(
        pagerank_pipeline_test_data.test_name, ctx.dialect
    )
    pagerank_pipeline_test_data.run_sql_test(
        get_pagerank_graph, file_path, update_tests, ctx
    )


@pytest.mark.execute
def test_pipeline_e2e_pagerank(
    pagerank_pipeline_test_data: PyDoughPandasTest,
    get_pagerank_graph: graph_fetcher,
    sqlite_pagerank_db_contexts: dict[str, DatabaseContext],
):
    """
    Verifies the final output answer for the pagerank tests. The outputs were
    generated using this website: https://pagerank-visualizer.netlify.app/.
    """
    pagerank_pipeline_test_data.run_e2e_test(
        get_pagerank_graph,
        sqlite_pagerank_db_contexts[pagerank_pipeline_test_data.graph_name],
    )
