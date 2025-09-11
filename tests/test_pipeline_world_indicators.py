"""
Integration tests for the PyDough workflow with custom questions on the custom
world development indicators dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                "result = world_development_indicators.Country.WHERE("
                "    (IncomeGroup == 'Low income') &"
                "    HAS(CountryNotes.WHERE(Series.SeriesCode == 'DT.DOD.DECT.CD'))"
                ").CALCULATE("
                "    country_code=CountryCode"
                ")",
                "world_development_indicators",
                lambda: pd.DataFrame(
                    {
                        "condition_description": ["Viral sinusitis (disorder)"],
                    }
                ),
                "wdi_low_income_country_with_series",
            ),
            id="most_common_conditions",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = world_development_indicators.Country.WHERE("
                "    ShortName == 'Albania'"
                ").Footnotes.WHERE("
                "    Year == '1981'"
                ").CALCULATE("
                "    footnote_description=Description"
                ")",
                "world_development_indicators",
                lambda: pd.DataFrame(
                    {
                        "condition_description": ["Viral sinusitis (disorder)"],
                    }
                ),
                "wdi_albania_footnotes_1981",
            ),
            id="most_common_conditions",
        ),
    ],
)
def world_indicators_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests using epoch test data. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


def test_pipeline_until_relational_world_indicators(
    world_indicators_pipeline_test_data: PyDoughPandasTest,
    get_test_graph_by_name: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom world development
    indicators dataset into relational plans.
    """
    file_path: str = get_plan_test_filename(
        world_indicators_pipeline_test_data.test_name
    )
    world_indicators_pipeline_test_data.run_relational_test(
        get_test_graph_by_name, file_path, update_tests
    )


def test_pipeline_until_sql_world_indicators(
    world_indicators_pipeline_test_data: PyDoughPandasTest,
    get_test_graph_by_name: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the PyDough queries on the custom world development
    indicators dataset into SQL text.
    """
    file_path: str = get_sql_test_filename(
        world_indicators_pipeline_test_data.test_name, empty_context_database.dialect
    )
    world_indicators_pipeline_test_data.run_sql_test(
        get_test_graph_by_name,
        file_path,
        update_tests,
        empty_context_database,
    )


@pytest.mark.execute
@pytest.mark.skip(reason="Missing alias issue needs to be fixed")
def test_pipeline_e2e_world_indicators(
    world_indicators_pipeline_test_data: PyDoughPandasTest,
    get_test_graph_by_name: graph_fetcher,
    sqlite_world_indicators_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom world development
    indicators dataset against the refsol DataFrame.
    """
    world_indicators_pipeline_test_data.run_e2e_test(
        get_test_graph_by_name,
        sqlite_world_indicators_connection,
    )
