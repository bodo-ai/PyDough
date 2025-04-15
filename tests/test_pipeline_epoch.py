"""
Integration tests for the PyDough workflow with custom questions on the custom
EPOCH dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from epoch_pydough_functions import (
    culture_events_info,
    event_gap_per_era,
    events_per_season,
    first_event_per_era,
    num_predawn_cold_war,
    summer_events_per_type,
)
from test_utils import graph_fetcher

from pydough import init_pydough_context, to_df
from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.database_connectors import DatabaseContext
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
                        "era_name": ["Summer", "Winter", "Fall", "Spring"],
                        "event_name": [48, 48, 45, 36],
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
