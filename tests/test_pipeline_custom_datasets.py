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
                        "condition_description": ["Normal pregnancy"],
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
        #  uncomment and align to reproduce SQL query optimization failed. SQL generation works for SQLite
        #         pytest.param(
        #             PyDoughPandasTest(
        #                 r"""
        # result = cast_.WHERE(
        #     (lowercase_detail_3._0_0_and == '2 "0 = 0 and \'" field name') & (lowercase_detail_4.id_ == 1)
        # ).CALCULATE(
        #     id1=id2,
        #     id2=id_,
        #     # uncomment to reproduce ERROR WHILE OPTIMIZING QUERY one more time
        #     # fk1_select=lowercase_detail_3.select_,
        #     # fk1_as=lowercase_detail_3.as_,
        #     fk2_two_words=lowercase_detail_4.two_words
        # )
        #                 """,
        #                 "keywords",
        #                 lambda: pd.DataFrame(
        #                     {
        #                         "id1": [2],
        #                         "id2": [1],
        #                         # "fk1_select": ["2 select reserved word"],
        #                         # "fk1_as": ["2 as reserved word"],
        #                         "fk2_two_words": ["1 two words field name"],
        #                     }
        #                 ),
        #                 "keywords_cast_alias_and_missing_alias",
        #             ),
        #             id="keywords_cast_alias_and_missing_alias",
        #         ),
        pytest.param(
            PyDoughPandasTest(
                r"""
result = master.WHERE(
    (id1 == 1) & (id2 == 1) & (description != 'One-One \'master row')
).CALCULATE(description=description)
                """,
                "keywords",
                lambda: pd.DataFrame(
                    {
                        "description": ["One-One master row"],
                    }
                ),
                "keywords_single_quote_use",
            ),
            id="keywords_single_quote_use",
        ),
        #  uncomment and align to reproduce SQL query optimization failed. SQL execution works for SQLite
        #         pytest.param(
        #             PyDoughPandasTest(
        #                 r'''
        # result = mixedcase_1_1.WHERE(
        #     (parentheses == '5 (parentheses)') & (lowercase_detail_5.as_ == '10 as reserved word')
        # ).CALCULATE(
        #     id_=id_,
        #     LowerCaseID=lowercaseid,
        #     integer=uppercase_master_2.integer,
        #     # uncomment to reproduce ERROR WHILE OPTIMIZING QUERY one more time
        #     #as_=lowercase_detail_5.as_,
        #     # replace order_ to order to reproduce column alias reserved issue
        #     order_=uppercase_master_2.order_by_
        # )
        #                 ''',
        #                 "keywords",
        #                 lambda: pd.DataFrame(
        #                     {
        #                         "id_": [5],
        #                         "LowerCaseID": [10],
        #                         "INTEGER": ["5 INTEGER RESERVED WORD"],
        #                         # "as_": ["10 as reserved word"],
        #                         "order_": ["5 TWO WORDS RESERVED"],
        #                     }
        #                 ),
        #                 "keywords_column_alias_reserved",
        #             ),
        #             id="keywords_column_alias_reserved",
        #         ),
        pytest.param(
            PyDoughPandasTest(
                r"""
result = count.WHERE(
    int_==8051
).CALCULATE(
    #dbl_quote_dot=unknown_column_6,
    dot=unknown_column_7,
    addition=(unknown_column_7+DEFAULT_TO(float_,str_,1)),
    col=col, 
    col1=col1,
    def_=def_,
    __del__=del_,
    __init__=init
)
                """,
                "keywords",
                lambda: pd.DataFrame(
                    {
                        # "dbl_quote_dot": [5051],
                        "dot": [6051],
                        "addition": [6052],
                        "col": [10051],
                        "col1": [11051],
                        "def_": [2051],
                        "__del__": [None],
                        "__init__": [7051],
                    }
                ),
                "keywords_python_sql_reserved",
            ),
            id="keywords_python_sql_reserved",
        ),
        #  uncomment and align to reproduce SQL query optimization failed. SQL generation works for SQLite
        #         pytest.param(
        #             PyDoughPandasTest(
        #                 r"""
        # result = where_.WHERE(
        #     (calculate_ == 4) & ABSENT(present)
        # ).CALCULATE(
        #     calculate=DEFAULT_TO(default_to,calculate_),
        #     _where=calculate__2.where_,
        #     _like=calculate__2.like_,
        #     datetime=calculate__2.datetime,
        #     abs=abs_,
        #     has=has
        # )
        #                 """,
        #                 "keywords",
        #                 lambda: pd.DataFrame(
        #                     {
        #                         "calculate": [4],
        #                         "_where": [4],
        #                         "_like": [None],
        #                         "DATETIME": [None],
        #                         "ABS": [None],
        #                         "HAS": [None],
        #                     }
        #                 ),
        #                 "keywords_alias_reserved_word",
        #             ),
        #             id="keywords_alias_reserved_word",
        #         ),
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
def test_pipeline_e2e_custom_datasets(
    custom_datasets_test_data: PyDoughPandasTest,
    get_test_graph_by_name: graph_fetcher,
    sqlite_custom_datasets_connection: DatabaseContext,
    coerce_types=True,
):
    """
    Test executing the the custom queries with the custom datasets against the
    refsol DataFrame.
    """
    custom_datasets_test_data.run_e2e_test(
        get_test_graph_by_name,
        sqlite_custom_datasets_connection,
    )
