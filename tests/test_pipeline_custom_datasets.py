"""
Integration tests for the PyDough workflow with custom questions on diverse
datasets.
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
                "result = patients.WHERE("
                "    (gender == 'F') & (ethnicity == 'american')"
                ").conditions.PARTITION("
                "    name='condition_groups',"
                "    by=DESCRIPTION"
                ").CALCULATE("
                "    condition_description=DESCRIPTION,"
                "    occurrence_count=COUNT(conditions)"
                ").TOP_K("
                "    1,"
                "    by=occurrence_count.DESC()"
                ").CALCULATE("
                "    condition_description"
                ")",
                "synthea",
                lambda: pd.DataFrame(
                    {
                        "condition_description": ["Viral sinusitis (disorder)"],
                    }
                ),
                "synthea_most_common_conditions",
            ),
            id="synthea_most_common_conditions",
        ),
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
            id="wdi_low_income_country_with_series",
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
            id="wdi_albania_footnotes_1981",
        ),
    ],
)
def custom_datasets_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests using epoch test data. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


def test_pipeline_until_relational_custom_datasets(
    custom_datasets_test_data: PyDoughPandasTest,
    get_test_graph_by_name: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom datasets into
    relational plans.
    """
    file_path: str = get_plan_test_filename(custom_datasets_test_data.test_name)
    custom_datasets_test_data.run_relational_test(
        get_test_graph_by_name, file_path, update_tests
    )


def test_pipeline_until_sql_custom_datasets(
    custom_datasets_test_data: PyDoughPandasTest,
    get_test_graph_by_name: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the PyDough queries on the custom datasets into
    SQL text.
    """
    file_path: str = get_sql_test_filename(
        custom_datasets_test_data.test_name, empty_context_database.dialect
    )
    custom_datasets_test_data.run_sql_test(
        get_test_graph_by_name,
        file_path,
        update_tests,
        empty_context_database,
    )


@pytest.mark.execute
# @pytest.mark.skip(reason="Missing alias issue needs to be fixed")
def test_pipeline_e2e_custom_datasets(
    custom_datasets_test_data: PyDoughPandasTest,
    get_test_graph_by_name: graph_fetcher,
    sqlite_custom_datasets_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom datasets against the
    refsol DataFrame.
    """
    custom_datasets_test_data.run_e2e_test(
        get_test_graph_by_name,
        sqlite_custom_datasets_connection,
    )
