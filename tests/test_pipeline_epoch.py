"""
Integration tests for the PyDough workflow with custom questions on the custom
EPOCH dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest

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
from tests.testing_utilities import graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            (
                first_event_per_era,
                "first_event_per_era",
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
            ),
            id="first_event_per_era",
        ),
        pytest.param(
            (
                events_per_season,
                "events_per_season",
                lambda: pd.DataFrame(
                    {
                        "season_name": ["Summer", "Winter", "Spring", "Fall"],
                        "n_events": [48, 48, 45, 36],
                    }
                ),
            ),
            id="events_per_season",
        ),
        pytest.param(
            (
                summer_events_per_type,
                "summer_events_per_type",
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
            ),
            id="summer_events_per_type",
        ),
        pytest.param(
            (
                num_predawn_cold_war,
                "num_predawn_cold_war",
                lambda: pd.DataFrame(
                    {
                        "n_events": [6],
                    }
                ),
            ),
            id="num_predawn_cold_war",
        ),
        pytest.param(
            (
                culture_events_info,
                "culture_events_info",
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
            ),
            id="culture_events_info",
        ),
        pytest.param(
            (
                event_gap_per_era,
                "event_gap_per_era",
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
            ),
            id="event_gap_per_era",
        ),
        pytest.param(
            (
                pct_searches_per_tod,
                "pct_searches_per_tod",
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
            ),
            id="pct_searches_per_tod",
        ),
        pytest.param(
            (
                users_most_cold_war_searches,
                "users_most_cold_war_searches",
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
            ),
            id="users_most_cold_war_searches",
        ),
        pytest.param(
            (
                intra_season_searches,
                "intra_season_searches",
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
            ),
            id="intra_season_searches",
        ),
        pytest.param(
            (
                most_popular_topic_per_region,
                "most_popular_topic_per_region",
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
            ),
            id="most_popular_topic_per_region",
        ),
        pytest.param(
            (
                most_popular_search_engine_per_tod,
                "most_popular_search_engine_per_tod",
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
            ),
            id="most_popular_search_engine_per_tod",
        ),
        pytest.param(
            (
                unique_users_per_engine,
                "unique_users_per_engine",
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
            ),
            id="unique_users_per_engine",
        ),
        pytest.param(
            (
                overlapping_event_search_other_users_per_user,
                "overlapping_event_search_other_users_per_user",
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
            ),
            id="overlapping_event_search_other_users_per_user",
        ),
        pytest.param(
            (
                overlapping_event_searches_per_user,
                "overlapping_event_searches_per_user",
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
            ),
            id="overlapping_event_searches_per_user",
        ),
        pytest.param(
            (
                search_results_by_tod,
                "search_results_by_tod",
                lambda: pd.DataFrame(
                    {
                        "tod": ["Pre-Dawn", "Morning", "Afternoon", "Evening", "Night"],
                        "pct_searches": [13.22, 34.71, 31.40, 11.57, 9.09],
                        "avg_results": [1624.88, 2123.93, 2863.45, 1992.93, 1659.18],
                    }
                ),
            ),
            id="search_results_by_tod",
        ),
    ],
)
def pydough_pipeline_test_data_epoch(
    request,
) -> tuple[
    Callable[[], UnqualifiedNode],
    str,
    Callable[[], pd.DataFrame],
]:
    """
    Test data for e2e tests using epoch test data. Returns a tuple of the
    following arguments:
    1. `unqualified_impl`: a function that takes in an unqualified root and
    creates the unqualified node for the TPCH query.
    2. `file_name`: the name of the file containing the expected relational
    plan.
    3. `answer_impl`: a function that takes in nothing and returns the answer
    to a TPCH query as a Pandas DataFrame.
    """
    return request.param


def test_pipeline_until_relational_epoch(
    pydough_pipeline_test_data_epoch: tuple[
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
    unqualified_impl, file_name, _ = pydough_pipeline_test_data_epoch
    file_path: str = get_plan_test_filename(file_name)
    graph: GraphMetadata = get_sample_graph("Epoch")
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


def test_pipeline_until_sql_epoch(
    pydough_pipeline_test_data_epoch: tuple[
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
    unqualified_impl, test_name, _ = pydough_pipeline_test_data_epoch
    file_name: str = f"epoch_{test_name}"
    file_path: str = get_sql_test_filename(file_name, empty_context_database.dialect)
    graph: GraphMetadata = get_sample_graph("Epoch")
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
def test_pipeline_e2e_epoch(
    pydough_pipeline_test_data_epoch: tuple[
        Callable[[], UnqualifiedNode],
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    sqlite_epoch_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom epoch dataset against
    the refsol DataFrame.
    """
    unqualified_impl, _, answer_impl = pydough_pipeline_test_data_epoch
    graph: GraphMetadata = get_sample_graph("Epoch")
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(root, metadata=graph, database=sqlite_epoch_connection)
    pd.testing.assert_frame_equal(result, answer_impl())
