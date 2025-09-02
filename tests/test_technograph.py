"""
Integration tests for the PyDough workflow with custom questions on the custom
TechnoGraph dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.test_pydough_functions.technograph_pydough_functions import (
    battery_failure_rates_anomalies,
    country_cartesian_oddball,
    country_combination_analysis,
    country_incident_rate_analysis,
    error_percentages_sun_set_by_error,
    error_rate_sun_set_by_factory_country,
    global_incident_rate,
    hot_purchase_window,
    incident_rate_by_release_year,
    incident_rate_per_brand,
    monthly_incident_rate,
    most_unreliable_products,
    year_cumulative_incident_rate_goldcopperstar,
    year_cumulative_incident_rate_overall,
)

from .testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                global_incident_rate,
                "TechnoGraph",
                lambda: pd.DataFrame({"ir": [2.41]}),
                "technograph_global_incident_rate",
            ),
            id="global_incident_rate",
        ),
        pytest.param(
            PyDoughPandasTest(
                incident_rate_per_brand,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "brand": [
                            "AstralSea",
                            "Dreadnought",
                            "OrangeRectangle",
                            "Zenith",
                        ],
                        "ir": [2.58, 2.58, 2.23, 2.39],
                    }
                ),
                "technograph_incident_rate_per_brand",
            ),
            id="incident_rate_per_brand",
        ),
        pytest.param(
            PyDoughPandasTest(
                most_unreliable_products,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "product": [
                            "Sun-Set",
                            "SapphireBolt-Flare",
                            "EmeraldScream-Vision",
                            "Void-Flare",
                            "OnyxCopper-Fox",
                        ],
                        "product_brand": [
                            "AstralSea",
                            "Zenith",
                            "AstralSea",
                            "Dreadnought",
                            "OrangeRectangle",
                        ],
                        "product_type": [
                            "laptop",
                            "tablet",
                            "laptop",
                            "phone",
                            "phone",
                        ],
                        "ir": [15.80, 15.08, 13.19, 12.63, 11.95],
                    }
                ),
                "technograph_most_unreliable_products",
            ),
            id="most_unreliable_products",
        ),
        pytest.param(
            PyDoughPandasTest(
                incident_rate_by_release_year,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "year": [
                            2013,
                            2014,
                            2015,
                            2016,
                            2017,
                            2018,
                            2019,
                            2020,
                            2021,
                            2022,
                            2023,
                            2024,
                        ],
                        "ir": [
                            2.34,
                            4.82,
                            3.79,
                            4.14,
                            3.36,
                            2.58,
                            1.89,
                            2.70,
                            1.48,
                            0.84,
                            0.32,
                            0.35,
                        ],
                    }
                ),
                "technograph_incident_rate_by_release_year",
            ),
            id="incident_rate_by_release_year",
        ),
        pytest.param(
            PyDoughPandasTest(
                error_rate_sun_set_by_factory_country,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "country": ["CA", "CN", "FR", "JP", "MX", "US"],
                        "ir": [14.0, 19.4, 18.5, 5.0, 14.17, 22.5],
                    }
                ),
                "technograph_error_rate_sun_set_by_factory_country",
            ),
            id="error_rate_sun_set_by_factory_country",
        ),
        pytest.param(
            PyDoughPandasTest(
                error_percentages_sun_set_by_error,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "error": [
                            "Charging Port Issue",
                            "Overheating",
                            "Cracked Screen",
                            "Display Issue",
                            "Software Bug",
                            "Microphone Issue",
                            "Network Issue",
                            "Speaker Issue",
                            "Camera Malfunction",
                            "Battery Failure",
                        ],
                        "pct": [
                            38.61,
                            22.78,
                            19.94,
                            5.7,
                            4.75,
                            2.85,
                            2.53,
                            1.58,
                            1.27,
                            0.00,
                        ],
                    }
                ),
                "technograph_error_percentages_sun_set_by_error",
            ),
            id="error_percentages_sun_set_by_error",
        ),
        pytest.param(
            PyDoughPandasTest(
                battery_failure_rates_anomalies,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "country_name": ["MX", "CA", "CA", "MX", "FR"],
                        "product_name": [
                            "RubyVoid-III",
                            "SapphireBolt-Flare",
                            "GoldBolt-Flare",
                            "Void-Flare",
                            "OnyxBeat-II",
                        ],
                        "ir": [17.0, 15.57, 15.0, 15.0, 14.43],
                    }
                ),
                "technograph_battery_failure_rates_anomalies",
            ),
            id="battery_failure_rates_anomalies",
        ),
        pytest.param(
            PyDoughPandasTest(
                country_incident_rate_analysis,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "country_name": ["CA", "CN", "FR", "JP", "MX", "US"],
                        "made_ir": [2.75, 3.89, 2.22, 0.93, 2.52, 1.54],
                        "sold_ir": [2.52, 3.42, 2.40, 1.56, 2.18, 1.74],
                        "user_ir": [2.45, 3.20, 2.48, 1.56, 2.09, 1.91],
                    }
                ),
                "technograph_country_incident_rate_analysis",
            ),
            id="country_incident_rate_analysis",
        ),
        pytest.param(
            PyDoughPandasTest(
                year_cumulative_incident_rate_goldcopperstar,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "years_since_release": range(13),
                        "cum_ir": [
                            None,
                            0.0,
                            0.14,
                            0.22,
                            0.13,
                            0.15,
                            0.31,
                            0.59,
                            0.59,
                            0.69,
                            0.71,
                            0.91,
                            1.0,
                        ],
                        "pct_bought_change": [
                            None,
                            None,
                            150.0,
                            -60.0,
                            250.0,
                            -42.86,
                            50.0,
                            -50.0,
                            0.0,
                            0.0,
                            -100.0,
                            None,
                            None,
                        ],
                        "pct_incident_change": [
                            None,
                            None,
                            None,
                            0.0,
                            -100.0,
                            None,
                            400.0,
                            80.0,
                            -77.78,
                            150.0,
                            -80.0,
                            600.0,
                            -57.14,
                        ],
                        "bought": [0, 2, 5, 2, 7, 4, 6, 3, 3, 3, 0, 0, 0],
                        "incidents": [0, 0, 1, 1, 0, 1, 5, 9, 2, 5, 1, 7, 3],
                    }
                ),
                "technograph_year_cumulative_incident_rate_goldcopperstar",
            ),
            id="year_cumulative_incident_rate_goldcopperstar",
        ),
        pytest.param(
            PyDoughPandasTest(
                year_cumulative_incident_rate_overall,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "yr": range(2014, 2025),
                        "cum_ir": [
                            0.33,
                            0.66,
                            0.87,
                            1.18,
                            1.4,
                            1.68,
                            1.9,
                            2.07,
                            2.18,
                            2.05,
                            2.06,
                        ],
                        "pct_bought_change": [
                            None,
                            175.0,
                            110.61,
                            59.71,
                            34.68,
                            16.72,
                            12.32,
                            5.36,
                            9.69,
                            25.83,
                            0.53,
                        ],
                        "pct_incident_change": [
                            None,
                            537.5,
                            174.51,
                            138.57,
                            54.19,
                            55.53,
                            22.47,
                            13.97,
                            5.64,
                            -26.76,
                            40.12,
                        ],
                        "bought": [24, 66, 139, 222, 299, 349, 392, 413, 453, 570, 573],
                        "incidents": [
                            8,
                            51,
                            140,
                            334,
                            515,
                            801,
                            981,
                            1118,
                            1181,
                            865,
                            1212,
                        ],
                    }
                ),
                "technograph_year_cumulative_incident_rate_overall",
            ),
            id="year_cumulative_incident_rate_overall",
        ),
        pytest.param(
            PyDoughPandasTest(
                hot_purchase_window,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "start_of_period": ["2024-04-30"],
                        "n_purchases": [25],
                    }
                ),
                "technograph_hot_purchase_window",
            ),
            id="hot_purchase_window",
        ),
        pytest.param(
            PyDoughPandasTest(
                country_combination_analysis,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "factory_country": ["CN", "CN", "CN", "CA", "MX"],
                        "purchase_country": ["JP", "FR", "US", "JP", "JP"],
                        "ir": [5.46, 5.27, 4.63, 4.07, 4.02],
                    }
                ),
                "technograph_country_combination_analysis",
            ),
            id="country_combination_analysis",
        ),
        pytest.param(
            PyDoughPandasTest(
                country_cartesian_oddball,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "name": ["CA", "CN", "FR", "JP", "MX", "US"],
                        "n_other_countries": [6] * 6,
                    }
                ),
                "technograph_country_cartesian_oddball",
            ),
            id="country_cartesian_oddball",
        ),
        pytest.param(
            PyDoughPandasTest(
                monthly_incident_rate,
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "month": [
                            f"{year}-{month:02d}"
                            for year in range(2020, 2022)
                            for month in range(1, 13)
                        ],
                        "ir": [
                            1654.39,
                            1785.71,
                            1310.47,
                            1488.1,
                            1453.0,
                            1583.76,
                            1903.77,
                            2061.73,
                            2411.87,
                            2052.77,
                            2269.29,
                            2284.94,
                            2059.54,
                            2576.06,
                            2103.12,
                            2604.51,
                            2560.57,
                            3041.4,
                            2405.83,
                            3283.27,
                            2478.68,
                            2694.04,
                            3474.37,
                            3048.78,
                        ],
                    }
                ),
                "technograph_monthly_incident_rate",
            ),
            id="monthly_incident_rate",
        ),
        pytest.param(
            PyDoughPandasTest(
                "global_info = TechnoGraph.CALCULATE(selected_date=products.WHERE(name == 'AmethystCopper-I').SINGULAR().release_date)\n"
                "selected_countries = countries.WHERE(~CONTAINS(name, 'C')).CALCULATE(country_name=name).PARTITION(name='country', by=country_name).CALCULATE(country_name)\n"
                "selected_days = global_info.calendar.WHERE((calendar_day >= selected_date) & (calendar_day < DATETIME(selected_date, '+2 years'))).CALCULATE(start_of_year=DATETIME(calendar_day, 'start of year')).PARTITION(name='months', by=start_of_year)\n"
                "combos = selected_countries.CROSS(selected_days)\n"
                "result = combos.CALCULATE(country_name, start_of_year).ORDER_BY(country_name.ASC(), start_of_year.ASC())",
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "country_name": ["FR"] * 3
                        + ["JP"] * 3
                        + ["MX"] * 3
                        + ["US"] * 3,
                        "start_of_year": ["2020-01-01", "2021-01-01", "2022-01-01"] * 4,
                    }
                ),
                "country_x_week_combos",
            ),
            id="country_x_week_combos",
        ),
        pytest.param(
            PyDoughPandasTest(
                "global_info = TechnoGraph.CALCULATE(selected_date=products.WHERE(name == 'AmethystCopper-I').SINGULAR().release_date)\n"
                "selected_countries = countries.WHERE(~CONTAINS(name, 'C')).CALCULATE(country_name=name).PARTITION(name='country', by=country_name).CALCULATE(country_name)\n"
                "selected_days = global_info.calendar.WHERE((calendar_day >= selected_date) & (calendar_day < DATETIME(selected_date, '+2 years'))).CALCULATE(start_of_year=DATETIME(calendar_day, 'start of year')).PARTITION(name='months', by=start_of_year)\n"
                "combos = selected_countries.CROSS(selected_days)\n"
                "result = combos.CALCULATE("
                " country_name,"
                " start_of_year,"
                " n_purchases=COUNT(calendar.devices_sold.WHERE((product.name == 'AmethystCopper-I') & (purchase_country.name == country_name))),"
                ").ORDER_BY(country_name.ASC(), start_of_year.ASC())",
                "TechnoGraph",
                lambda: pd.DataFrame(
                    {
                        "country_name": ["FR"] * 3
                        + ["JP"] * 3
                        + ["MX"] * 3
                        + ["US"] * 3,
                        "start_of_year": ["2020-01-01", "2021-01-01", "2022-01-01"] * 4,
                        "n": [1, 1, 0, 0, 5, 2, 1, 1, 1, 0, 0, 0],
                    }
                ),
                "country_x_week_analysis",
            ),
            id="country_x_week_analysis",
        ),
    ],
)
def technograph_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests using technograph test data. Returns an
    instance of PyDoughPandasTest containing information about the test.
    """
    return request.param


def test_pipeline_until_relational_technograph(
    technograph_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom epoch dataset
    into relational plans.
    """
    file_path: str = get_plan_test_filename(technograph_pipeline_test_data.test_name)
    technograph_pipeline_test_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )


def test_pipeline_until_sql_technograph(
    technograph_pipeline_test_data: PyDoughPandasTest,
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
    file_path: str = get_sql_test_filename(
        technograph_pipeline_test_data.test_name,
        empty_context_database.dialect,
    )
    technograph_pipeline_test_data.run_sql_test(
        get_sample_graph, file_path, update_tests, empty_context_database
    )


@pytest.mark.execute
def test_pipeline_e2e_technograph(
    technograph_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_technograph_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom technograph dataset
    against the refsol DataFrame.
    """
    technograph_pipeline_test_data.run_e2e_test(
        get_sample_graph, sqlite_technograph_connection
    )
