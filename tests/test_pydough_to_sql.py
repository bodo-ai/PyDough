"""
Test that tests the full conversion of a PyDough object to a SQL query.
"""

from collections.abc import Callable

import pytest

from pydough import init_pydough_context, to_sql
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)
from tests.test_pydough_functions.all_pydough_functions_dialects import (
    aggregation_functions,
    arithmetic_and_binary_operators,
    casting_functions,
    comparisons_and_logical_operators,
    conditional_functions,
    datetime_functions,
    numerical_functions,
    string_functions,
    unary_and_slicing_operators,
    window_functions,
)
from tests.test_pydough_functions.simple_pydough_functions import (
    cumulative_stock_analysis,
    datediff,
    datetime_sampler,
    extract_colors,
    floor_and_ceil,
    floor_and_ceil_2,
    get_part_multiple,
    get_part_single,
    global_acctbal_breakdown,
    hour_minute_day,
    nation_acctbal_breakdown,
    quantile_function_test_1,
    quantile_function_test_2,
    rank_a,
    rank_b,
    rank_c,
    region_acctbal_breakdown,
    simple_filter,
    simple_scan,
    simple_smallest_or_largest,
    simple_var_std,
    time_threshold_reached,
    transaction_week_sampler,
    week_offset,
    window_sliding_frame_relsize,
    window_sliding_frame_relsum,
)
from tests.test_pydough_functions.user_collections import (
    simple_range_1,
    simple_range_2,
    simple_range_3,
    simple_range_4,
    simple_range_5,
    user_range_collection_1,
    user_range_collection_2,
    user_range_collection_3,
    user_range_collection_4,
)
from tests.testing_utilities import (
    graph_fetcher,
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
            floor_and_ceil,
            None,
            "floor_and_ceil",
            id="floor_and_ceil",
        ),
        pytest.param(
            floor_and_ceil_2,
            None,
            "floor_and_ceil_2",
            id="floor_and_ceil_2",
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
        pytest.param(
            simple_var_std,
            None,
            "simple_var_std",
            id="simple_var_std",
        ),
        pytest.param(
            extract_colors,
            None,
            "extract_colors",
            id="extract_colors",
        ),
        pytest.param(
            quantile_function_test_1, None, "quantile_test_1", id="quantile_test_1"
        ),
        pytest.param(
            quantile_function_test_2, None, "quantile_test_2", id="quantile_test_2"
        ),
        pytest.param(
            arithmetic_and_binary_operators,
            None,
            "arithmetic_and_binary_operators",
            id="arithmetic_and_binary_operators",
        ),
        pytest.param(
            comparisons_and_logical_operators,
            None,
            "comparisons_and_logical_operators",
            id="comparisons_and_logical_operators",
        ),
        pytest.param(
            unary_and_slicing_operators,
            None,
            "unary_and_slicing_operators",
            id="unary_and_slicing_operators",
        ),
        pytest.param(string_functions, None, "string_functions", id="string_functions"),
        pytest.param(
            datetime_functions, None, "datetime_functions", id="datetime_functions"
        ),
        pytest.param(
            conditional_functions,
            None,
            "conditional_functions",
            id="conditional_functions",
        ),
        pytest.param(
            numerical_functions, None, "numerical_functions", id="numerical_functions"
        ),
        pytest.param(
            aggregation_functions,
            None,
            "aggregation_functions",
            id="aggregation_functions",
        ),
        pytest.param(window_functions, None, "window_functions", id="window_functions"),
        pytest.param(
            casting_functions, None, "casting_functions", id="casting_functions"
        ),
        pytest.param(simple_range_1, None, "simple_range_1", id="simple_range_1"),
        pytest.param(simple_range_2, None, "simple_range_2", id="simple_range_2"),
        pytest.param(simple_range_3, None, "simple_range_3", id="simple_range_3"),
        pytest.param(simple_range_4, None, "simple_range_4", id="simple_range_4"),
        pytest.param(simple_range_5, None, "simple_range_5", id="simple_range_5"),
        pytest.param(
            user_range_collection_1,
            None,
            "user_range_collection_1",
            id="user_range_collection_1",
        ),
        # TODO: FIXME
        pytest.param(
            user_range_collection_2,
            None,
            "user_range_collection_2",
            id="user_range_collection_2",
            marks=pytest.mark.skip(
                "ValueError: Context does not contain expression BACK(1).x. Available expressions: [y]"
            ),
        ),
        # TODO: FIXME
        pytest.param(
            user_range_collection_3,
            None,
            "user_range_collection_3",
            id="user_range_collection_3",
            marks=pytest.mark.skip(
                "PyDough nodes ENDSIWTH is not callable. Did you mean to use a function?"
            ),
        ),
        # TODO: FIXME
        pytest.param(
            user_range_collection_4,
            None,
            "user_range_collection_4",
            id="user_range_collection_4",
            marks=pytest.mark.skip("Cannot qualify NoneType: None"),
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
        pytest.param(
            window_sliding_frame_relsize,
            "window_sliding_frame_relsize",
            "Broker",
            id="window_sliding_frame_relsize",
        ),
        pytest.param(
            window_sliding_frame_relsum,
            "window_sliding_frame_relsum",
            "Broker",
            id="window_sliding_frame_relsum",
        ),
        pytest.param(
            get_part_single, "get_part_single", "Broker", id="get_part_single"
        ),
        pytest.param(
            get_part_multiple, "get_part_multiple", "Broker", id="get_part_multiple"
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
