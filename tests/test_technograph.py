"""
Integration tests for the PyDough workflow with custom questions on the custom
Technograph dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from technograph_pydough_functions import (
    battery_failure_rates_anomalies,
    country_incident_rate_analysis,
    error_percentages_sun_set_by_error,
    error_rate_sun_set_by_factory_country,
    global_incident_rate,
    incident_rate_by_release_year,
    incident_rate_per_brand,
    most_unreliable_products,
    year_cumulative_incident_rate_goldcopperstar,
    year_cumulative_incident_rate_overall,
)
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
                lambda: pd.DataFrame({"ir": [2.41]}),
            ),
            id="global_incident_rate",
        ),
        pytest.param(
            (
                incident_rate_per_brand,
                "incident_rate_per_brand",
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
            ),
            id="incident_rate_per_brand",
        ),
        pytest.param(
            (
                most_unreliable_products,
                "most_unreliable_products",
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
            ),
            id="most_unreliable_products",
        ),
        pytest.param(
            (
                incident_rate_by_release_year,
                "incident_rate_by_release_year",
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
            ),
            id="incident_rate_by_release_year",
        ),
        pytest.param(
            (
                error_rate_sun_set_by_factory_country,
                "error_rate_sun_set_by_factory_country",
                lambda: pd.DataFrame(
                    {
                        "country": ["CA", "CN", "FR", "JP", "MX", "US"],
                        "ir": [14.0, 19.4, 18.5, 5.0, 14.17, 22.5],
                    }
                ),
            ),
            id="error_rate_sun_set_by_factory_country",
        ),
        pytest.param(
            (
                error_percentages_sun_set_by_error,
                "error_percentages_sun_set_by_error",
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
            ),
            id="error_percentages_sun_set_by_error",
        ),
        pytest.param(
            (
                battery_failure_rates_anomalies,
                "battery_failure_rates_anomalies",
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
            ),
            id="battery_failure_rates_anomalies",
        ),
        pytest.param(
            (
                country_incident_rate_analysis,
                "country_incident_rate_analysis",
                lambda: pd.DataFrame(
                    {
                        "country_name": ["CA", "CN", "FR", "JP", "MX", "US"],
                        "made_ir": [2.75, 3.89, 2.22, 0.93, 2.52, 1.54],
                        "sold_ir": [2.52, 3.42, 2.40, 1.56, 2.18, 1.74],
                        "user_ir": [2.45, 3.20, 2.48, 1.56, 2.09, 1.91],
                    }
                ),
            ),
            id="country_incident_rate_analysis",
        ),
        pytest.param(
            (
                year_cumulative_incident_rate_goldcopperstar,
                "year_cumulative_incident_rate_goldcopperstar",
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
            ),
            id="year_cumulative_incident_rate_goldcopperstar",
        ),
        pytest.param(
            (
                year_cumulative_incident_rate_overall,
                "year_cumulative_incident_rate_overall",
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
            ),
            id="year_cumulative_incident_rate_overall",
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
    file_name: str = f"technograph_{test_name}"
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
