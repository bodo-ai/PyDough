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
                """
result = (
    patients
    .WHERE((gender == 'F') & (ethnicity == 'italian'))
    .conditions
    .PARTITION(name='condition_groups', by=DESCRIPTION)
    .CALCULATE(condition_description=DESCRIPTION, occurrence_count=COUNT(conditions))
    .TOP_K(1, by=(occurrence_count.DESC(), condition_description.ASC()))
    .CALCULATE(condition_description)
)
                """,
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
                """
result = (
    wdi
    .Country
    .WHERE((IncomeGroup == 'Low income') & HAS(CountryNotes.WHERE(Series.SeriesCode == 'DT.DOD.DECT.CD')))
    .CALCULATE(country_code=CountryCode)
)
                """,
                "wdi",
                lambda: pd.DataFrame(
                    {
                        "country_code": [
                            "AFG",
                            "BDI",
                            "BEN",
                            "BFA",
                            "CAF",
                            "COM",
                            "ERI",
                            "ETH",
                            "GIN",
                            "GMB",
                            "GNB",
                            "HTI",
                            "KHM",
                            "LBR",
                            "MDG",
                            "MLI",
                            "MOZ",
                            "MWI",
                            "NER",
                            "NPL",
                            "RWA",
                            "SLE",
                            "SOM",
                            "TCD",
                            "TGO",
                            "TZA",
                            "UGA",
                            "ZAR",
                            "ZWE",
                        ],
                    }
                ),
                "wdi_low_income_country_with_series",
            ),
            id="wdi_low_income_country_with_series",
        ),
        pytest.param(
            PyDoughPandasTest(
                """
result = (
    wdi
    .Country
    .WHERE(ShortName == 'Albania')
    .Footnotes
    .WHERE(Year == 'YR2012')
    .CALCULATE(footnote_description=Description)
)
                """,
                "wdi",
                lambda: pd.DataFrame(
                    {
                        "condition_description": [
                            "As reported",
                            "Period: 2008-2012.Grouped consumption data.Growth rates are based on survey means of 2011 PPP$.Survey reference CPI years for the initial and final years are 2008 and 2012, respectively.",
                            "Source: Labour force survey. Coverage: Civilian. Coverage (unemployment): Not available. Age: 15-74. Coverage limitation: Excluding institutional population. Education: International Standard Classification of Education, 1997 version.",
                        ]
                    }
                ),
                "wdi_albania_footnotes_1978",
            ),
            id="wdi_albania_footnotes_1978",
        ),
    ],
)
def s3_datasets_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests using epoch test data. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


@pytest.mark.s3
def test_pipeline_until_relational_s3_datasets(
    s3_datasets_test_data: PyDoughPandasTest,
    get_s3_datasets_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom datasets into
    relational plans.
    """
    file_path: str = get_plan_test_filename(s3_datasets_test_data.test_name)
    s3_datasets_test_data.run_relational_test(
        get_s3_datasets_graph, file_path, update_tests
    )


@pytest.mark.s3
def test_pipeline_until_sql_custom_datasets(
    s3_datasets_test_data: PyDoughPandasTest,
    get_s3_datasets_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the PyDough queries on the custom datasets into
    SQL text.
    """
    file_path: str = get_sql_test_filename(
        s3_datasets_test_data.test_name, empty_context_database.dialect
    )
    s3_datasets_test_data.run_sql_test(
        get_s3_datasets_graph,
        file_path,
        update_tests,
        empty_context_database,
    )


@pytest.mark.s3
@pytest.mark.execute
def test_pipeline_e2e_custom_datasets(
    s3_datasets_test_data: PyDoughPandasTest,
    get_s3_datasets_graph: graph_fetcher,
    sqlite_s3_datasets_connection: Callable[[str], DatabaseContext],
):
    """
    Test executing the the custom queries with the custom datasets against the
    refsol DataFrame.
    """
    s3_datasets_test_data.run_e2e_test(
        get_s3_datasets_graph,
        sqlite_s3_datasets_connection(s3_datasets_test_data.graph_name.lower()),
        coerce_types=True,
    )
