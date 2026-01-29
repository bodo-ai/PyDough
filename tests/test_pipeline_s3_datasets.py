"""
Integration tests for the PyDough workflow with custom questions on diverse
s3 datasets.
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
    .PARTITION(name='condition_groups', by=description)
    .CALCULATE(condition_description=description, occurrence_count=COUNT(conditions))
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
    world_development_indicators
    .Country
    .WHERE((IncomeGroup == 'Low income') & HAS(CountryNotes.WHERE(Series.SeriesCode == 'DT.DOD.DECT.CD')))
    .CALCULATE(country_code=CountryCode)
)
                """,
                "world_development_indicators",
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
    world_development_indicators
    .Country
    .WHERE(ShortName == 'Albania')
    .Footnotes
    .WHERE(Year == 'YR2012')
    .CALCULATE(footnote_description=Description)
)
                """,
                "world_development_indicators",
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
        pytest.param(
            PyDoughPandasTest(
                """
result = menu.menu.WHERE(
    HAS(menupages.menuitems.dish.WHERE(LOWER(name) == "baked apples with cream"))
    ).CALCULATE(
        sponsor_name=sponsor,
        max_item_price=MAX(menupages.menuitems.price)
    ).TOP_K(
        1, by=max_item_price.DESC()
    ).CALCULATE(
        sponsor=sponsor_name
    )
            """,
                "menu",
                lambda: pd.DataFrame(
                    {
                        "sponsor": ["MURRAY HILL HOTEL"],
                    }
                ),
                "menu_5556",
            ),
            id="menu_5556",
        ),
        pytest.param(
            PyDoughPandasTest(
                """
result = donor.CALCULATE(
    total_sb_projects=COUNT(projects.WHERE(LOWER(school_city) == "santa barbara")),
    suburban_sb_projects=COUNT(projects.WHERE(
        (LOWER(school_city) == "santa barbara") &
        (LOWER(school_metro) == "suburban")
    ))
).CALCULATE(
    percentage_suburban=100.0 * suburban_sb_projects / total_sb_projects
)
            """,
                "donor",
                lambda: pd.DataFrame(
                    {
                        "percentage_suburban": [30.303],
                    }
                ),
                "donor_3276",
            ),
            id="donor_3276",
        ),
        pytest.param(
            PyDoughPandasTest(
                """
result = movielens.CALCULATE(
    # Count all users who have at least one rating of 2
    total_users_with_rating_2 = COUNT(users.WHERE(
        HAS(submitted_ratings.WHERE(rating == 2))
    )),
    # Count female users who have at least one rating of 2
    female_users_with_rating_2 = COUNT(users.WHERE(
        HAS(submitted_ratings.WHERE(rating == 2)) & (LOWER(u_gender) == 'f')
    ))
).CALCULATE(
    # Calculate the percentage, handling division by zero
    percentage_of_female_users = IFF(
        total_users_with_rating_2 > 0,
        100.0 * female_users_with_rating_2 / total_users_with_rating_2,
        0.0
    )
)
            """,
                "movielens",
                lambda: pd.DataFrame(
                    {
                        "percentage_of_female_users": [27.961],
                    }
                ),
                "movielens_2274",
            ),
            id="movielens_2274",
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
    Tests the conversion of the PyDough queries on the s3 datasets into
    relational plans.
    """
    file_path: str = get_plan_test_filename(s3_datasets_test_data.test_name)
    s3_datasets_test_data.run_relational_test(
        get_s3_datasets_graph, file_path, update_tests
    )


@pytest.mark.s3
def test_pipeline_until_sql_s3_datasets(
    s3_datasets_test_data: PyDoughPandasTest,
    get_s3_datasets_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the PyDough queries on the s3 datasets into
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
def test_pipeline_e2e_s3_datasets(
    s3_datasets_test_data: PyDoughPandasTest,
    get_s3_datasets_graph: graph_fetcher,
    sqlite_s3_datasets_connection: Callable[[str], DatabaseContext],
):
    """
    Test executing the e2e queries with the s3 datasets against the
    refsol DataFrame.
    """
    s3_datasets_test_data.run_e2e_test(
        get_s3_datasets_graph,
        sqlite_s3_datasets_connection(s3_datasets_test_data.graph_name.lower()),
        coerce_types=True,
    )
