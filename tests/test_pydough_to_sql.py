"""
Test that tests the full conversion of a PyDough object to a SQL query.
"""

from collections.abc import Callable

import pytest
from simple_pydough_functions import (
    cumulative_stock_analysis,
    datediff,
    datetime_sampler,
    global_acctbal_breakdown,
    hour_minute_day,
    nation_acctbal_breakdown,
    rank_a,
    rank_b,
    rank_c,
    region_acctbal_breakdown,
    simple_filter,
    simple_scan,
    simple_smallest_or_largest,
    time_threshold_reached,
    transaction_week_sampler,
    week_offset,
)
from test_utils import (
    graph_fetcher,
)

from pydough import init_pydough_context, to_sql
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)


@pytest.mark.parametrize(
    "pydough_code, columns, test_name",
    [
        pytest.param(
            simple_scan,
            None,
            "simple_scan",
            id="simple_scan",
        ),
        pytest.param(
            simple_filter,
            ["order_date", "o_orderkey", "o_totalprice"],
            "simple_filter",
            id="simple_filter",
        ),
        pytest.param(
            rank_a,
            {"id": "key", "rk": "rank"},
            "rank_a",
            id="rank_a",
        ),
        pytest.param(
            rank_b,
            {"order_key": "key", "rank": "rank"},
            "rank_b",
            id="rank_b",
        ),
        pytest.param(
            rank_c,
            None,
            "rank_c",
            id="rank_c",
        ),
        pytest.param(
            datetime_sampler,
            None,
            "datetime_sampler",
            id="datetime_sampler",
        ),
        pytest.param(
            nation_acctbal_breakdown,
            None,
            "nation_acctbal_breakdown",
            id="nation_acctbal_breakdown",
        ),
        pytest.param(
            region_acctbal_breakdown,
            None,
            "region_acctbal_breakdown",
            id="region_acctbal_breakdown",
        ),
        pytest.param(
            global_acctbal_breakdown,
            None,
            "global_acctbal_breakdown",
            id="global_acctbal_breakdown",
        ),
        pytest.param(
            simple_smallest_or_largest,
            None,
            "simple_smallest_or_largest",
            id="simple_smallest_or_largest",
        ),
    ],
)
def test_pydough_to_sql_tpch(
    pydough_code: Callable[[], UnqualifiedNode],
    columns: dict[str, str] | list[str] | None,
    test_name: str,
    get_sample_graph: graph_fetcher,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    empty_context_database: DatabaseContext,
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(pydough_code)()
    actual_sql: str = to_sql(
        root, columns=columns, metadata=graph, database=empty_context_database
    ).strip()
    file_path: str = get_sql_test_filename(test_name, empty_context_database.dialect)
    if update_tests:
        with open(file_path, "w") as f:
            f.write(actual_sql + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert actual_sql == expected_relational_string.strip(), (
            "Mismatch between tree generated SQL text and expected SQL text"
        )


@pytest.mark.parametrize(
    "pydough_code,test_name,graph_name",
    [
        pytest.param(
            hour_minute_day,
            "hour_minute_day",
            "Broker",
            id="hour_minute_day",
        ),
        pytest.param(
            datediff,
            "datediff",
            "Broker",
            id="datediff",
        ),
        pytest.param(
            week_offset,
            "week_offset",
            "Broker",
            id="week_offset",
        ),
        pytest.param(
            cumulative_stock_analysis,
            "cumulative_stock_analysis",
            "Broker",
            id="cumulative_stock_analysis",
        ),
        pytest.param(
            time_threshold_reached,
            "time_threshold_reached",
            "Broker",
            id="time_threshold_reached",
        ),
    ],
)
def test_pydough_to_sql_defog(
    pydough_code: Callable[[], UnqualifiedNode],
    test_name: str,
    graph_name: str,
    defog_graphs: graph_fetcher,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    empty_context_database: DatabaseContext,
    update_tests: bool,
    default_config: PyDoughConfigs,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    sql, with the correct string representation.
    """
    graph: GraphMetadata = defog_graphs(graph_name)
    root: UnqualifiedNode = init_pydough_context(graph)(pydough_code)()
    actual_sql: str = to_sql(
        root, metadata=graph, database=empty_context_database, config=default_config
    ).strip()
    file_path: str = get_sql_test_filename(test_name, empty_context_database.dialect)
    if update_tests:
        with open(file_path, "w") as f:
            f.write(actual_sql + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert actual_sql == expected_relational_string.strip(), (
            "Mismatch between tree generated SQL text and expected SQL text"
        )


def test_pydough_to_sql_defog_custom_week(
    defog_graphs: graph_fetcher,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    empty_context_database: DatabaseContext,
    update_tests: bool,
    week_handling_config: PyDoughConfigs,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    sql, with the correct string representation.
    """
    graph: GraphMetadata = defog_graphs("Broker")
    root: UnqualifiedNode = init_pydough_context(graph)(transaction_week_sampler)()
    test_name: str = "sql_transaction_week_sampler"
    test_name += f"_{week_handling_config.start_of_week.name.lower()}"
    test_name += f"_{'zero' if week_handling_config.start_week_as_zero else 'one'}"
    actual_sql: str = to_sql(
        root,
        metadata=graph,
        database=empty_context_database,
        config=week_handling_config,
    ).strip()
    file_path: str = get_sql_test_filename(test_name, empty_context_database.dialect)
    if update_tests:
        with open(file_path, "w") as f:
            f.write(actual_sql + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert actual_sql == expected_relational_string.strip(), (
            "Mismatch between tree generated SQL text and expected SQL text"
        )
