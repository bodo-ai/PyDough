"""
Integration tests for the PyDough workflow with custom questions on the custom
EPOCH dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.test_pydough_functions.epoch_pydough_functions import (
    culture_events_info,
    event_gap_per_era,
    events_per_season,
    first_event_per_era,
    intra_season_searches,
    most_popular_search_engine_per_tod,
    most_popular_topic_per_region,
    num_predawn_cold_war,
    overlapping_event_search_other_users_per_user,
    overlapping_event_searches_per_user,
    pct_searches_per_tod,
    search_results_by_tod,
    summer_events_per_type,
    unique_users_per_engine,
    users_most_cold_war_searches,
)
from tests.testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                first_event_per_era,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "era_name": [
                            "WWI",
                            "Interwar",
                            "WWII",
                            "Cold War",
                            "Modern Era",
                        ],
                        "event_name": [
                            "Assassination of Archduke Ferdinand",
                            "Founding of the League of Nations",
                            "Invasion of Poland",
                            "First Meeting of the United Nations General Assembly",
                            "Dissolution of the Soviet Union",
                        ],
                    }
                ),
                "epoch_first_event_per_era",
            ),
            id="first_event_per_era",
        ),
        pytest.param(
            PyDoughPandasTest(
                events_per_season,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "season_name": ["Summer", "Winter", "Spring", "Fall"],
                        "n_events": [48, 48, 45, 36],
                    }
                ),
                "epoch_events_per_season",
            ),
            id="events_per_season",
        ),
        pytest.param(
            PyDoughPandasTest(
                summer_events_per_type,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "event_type": [
                            "culture",
                            "death",
                            "natural_disaster",
                            "politics",
                            "science",
                            "technology",
                            "war",
                        ],
                        "n_events": [6, 3, 1, 14, 6, 1, 17],
                    }
                ),
                "epoch_summer_events_per_type",
            ),
            id="summer_events_per_type",
        ),
        pytest.param(
            PyDoughPandasTest(
                num_predawn_cold_war,
                "Epoch",
                lambda: pd.DataFrame({"n_events": [6]}),
                "epoch_num_predawn_cold_war",
            ),
            id="num_predawn_cold_war",
        ),
        pytest.param(
            PyDoughPandasTest(
                culture_events_info,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "event_name": [
                            'Charlie Chaplin Releases "The Tramp"',
                            "First Pulitzer Prizes Awarded",
                            "First Talking Motion Picture Released",
                            "Rosie the Riveter Campaign Begins",
                            "Anne Frank Goes into Hiding",
                            "Disneyland Opens",
                        ],
                        "era_name": [
                            "WWI",
                            "WWI",
                            "Interwar",
                            "WWII",
                            "WWII",
                            "Cold War",
                        ],
                        "event_year": [1915, 1917, 1927, 1942, 1942, 1955],
                        "season_name": [
                            "Spring",
                            "Summer",
                            "Fall",
                            "Winter",
                            "Summer",
                            "Summer",
                        ],
                        "tod": [
                            "Afternoon",
                            "Pre-Dawn",
                            "Pre-Dawn",
                            "Morning",
                            "Afternoon",
                            "Morning",
                        ],
                    }
                ),
                "epoch_culture_events_info",
            ),
            id="culture_events_info",
        ),
        pytest.param(
            PyDoughPandasTest(
                event_gap_per_era,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "era_name": [
                            "WWI",
                            "Interwar",
                            "WWII",
                            "Cold War",
                            "Modern Era",
                        ],
                        "avg_event_gap": [
                            59.354839,
                            304.928571,
                            75.733333,
                            350.065217,
                            221.509804,
                        ],
                    }
                ),
                "epoch_event_gap_per_era",
            ),
            id="event_gap_per_era",
        ),
        pytest.param(
            PyDoughPandasTest(
                pct_searches_per_tod,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "tod": [
                            "Pre-Dawn",
                            "Morning",
                            "Afternoon",
                            "Evening",
                            "Night",
                        ],
                        "pct_searches": [
                            13.22,
                            34.71,
                            31.40,
                            11.57,
                            9.09,
                        ],
                    }
                ),
                "epoch_pct_searches_per_tod",
            ),
            id="pct_searches_per_tod",
        ),
        pytest.param(
            PyDoughPandasTest(
                users_most_cold_war_searches,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "user_name": [
                            "Trixie Mattel",
                            "Jinkx Monsoon",
                            "Lady Camden",
                        ],
                        "n_cold_war_searches": [4, 3, 3],
                    }
                ),
                "epoch_users_most_cold_war_searches",
            ),
            id="users_most_cold_war_searches",
        ),
        pytest.param(
            PyDoughPandasTest(
                intra_season_searches,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "season_name": [
                            "Fall",
                            "Spring",
                            "Summer",
                            "Winter",
                        ],
                        "pct_season_searches": [4.00, 10.71, 24.14, 28.21],
                        "pct_event_searches": [9.09, 27.27, 33.33, 55.00],
                    }
                ),
                "epoch_intra_season_searches",
            ),
            id="intra_season_searches",
        ),
        pytest.param(
            PyDoughPandasTest(
                most_popular_topic_per_region,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "region": [
                            "Canada",
                            "UK",
                            "US-East",
                            "US-Midwest",
                            "US-South",
                            "US-West",
                        ],
                        "event_type": ["war"] + ["politics"] * 4 + ["technology"],
                        "n_searches": [3, 3, 4, 2, 3, 4],
                    }
                ),
                "epoch_most_popular_topic_per_region",
            ),
            id="most_popular_topic_per_region",
        ),
        pytest.param(
            PyDoughPandasTest(
                most_popular_search_engine_per_tod,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "tod": ["Afternoon", "Evening", "Morning", "Night", "Pre-Dawn"],
                        "search_engine": [
                            "Google",
                            "DuckDuckGo",
                            "Google",
                            "Brave",
                            "Google",
                        ],
                        "n_searches": [12, 4, 13, 3, 5],
                    }
                ),
                "epoch_most_popular_search_engine_per_tod",
            ),
            id="most_popular_search_engine_per_tod",
        ),
        pytest.param(
            PyDoughPandasTest(
                unique_users_per_engine,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "engine": [
                            "Bing",
                            "Brave",
                            "DuckDuckGo",
                            "Ecosia",
                            "Google",
                            "Mojeek",
                            "Qwant",
                            "Swisscows",
                            "Yahoo",
                            "Yandex",
                        ],
                        "n_users": [7, 3, 7, 5, 10, 0, 6, 0, 4, 2],
                    }
                ),
                "epoch_unique_users_per_engine",
            ),
            id="unique_users_per_engine",
        ),
        pytest.param(
            PyDoughPandasTest(
                overlapping_event_search_other_users_per_user,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "user_name": [
                            "Trixie Mattel",
                            "Jinkx Monsoon",
                            "Jorgeous",
                            "Lady Camden",
                            "Kandy Muse",
                            "Lemon",
                            "Plasma",
                        ],
                        "n_other_users": [7, 3, 3, 3, 2, 2, 2],
                    }
                ),
                "epoch_overlapping_event_search_other_users_per_user",
            ),
            id="overlapping_event_search_other_users_per_user",
        ),
        pytest.param(
            PyDoughPandasTest(
                overlapping_event_searches_per_user,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "user_name": [
                            "Trixie Mattel",
                            "Priyanka",
                            "Raja Gemini",
                            "Alyssa Edwards",
                        ],
                        "n_searches": [3, 2, 2, 1],
                    }
                ),
                "epoch_overlapping_event_searches_per_user",
            ),
            id="overlapping_event_searches_per_user",
        ),
        pytest.param(
            PyDoughPandasTest(
                search_results_by_tod,
                "Epoch",
                lambda: pd.DataFrame(
                    {
                        "tod": ["Pre-Dawn", "Morning", "Afternoon", "Evening", "Night"],
                        "pct_searches": [13.22, 34.71, 31.40, 11.57, 9.09],
                        "avg_results": [1624.88, 2123.93, 2863.45, 1992.93, 1659.18],
                    }
                ),
                "epoch_search_results_by_tod",
            ),
            id="search_results_by_tod",
        ),
    ],
)
def epoch_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests using epoch test data. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


def test_pipeline_until_relational_epoch(
    epoch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom epoch dataset
    into relational plans.
    """
    file_path: str = get_plan_test_filename(epoch_pipeline_test_data.test_name)
    epoch_pipeline_test_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )


def test_pipeline_until_sql_epoch(
    epoch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the PyDough queries on the custom epoch dataset
    into SQL text.
    """
    file_path: str = get_sql_test_filename(
        epoch_pipeline_test_data.test_name, empty_context_database.dialect
    )
    epoch_pipeline_test_data.run_sql_test(
        get_sample_graph,
        file_path,
        update_tests,
        empty_context_database,
    )


@pytest.mark.execute
def test_pipeline_e2e_epoch(
    epoch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_epoch_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom epoch dataset against
    the refsol DataFrame.
    """
    epoch_pipeline_test_data.run_e2e_test(
        get_sample_graph,
        sqlite_epoch_connection,
    )
