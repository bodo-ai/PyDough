"""
Test that tests the full conversion of a PyDough object to a SQL query.
"""

from collections.abc import Callable

import pytest
from simple_pydough_functions import (
    datediff,
    datetime_sampler,
    hour_minute_day,
    rank_a,
    rank_b,
    rank_c,
    simple_filter,
    simple_scan,
)
from test_utils import (
    graph_fetcher,
)

from pydough import init_pydough_context, to_sql
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
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    sql, with the correct string representation.
    """
    graph: GraphMetadata = defog_graphs(graph_name)
    root: UnqualifiedNode = init_pydough_context(graph)(pydough_code)()
    actual_sql: str = to_sql(
        root, metadata=graph, database=empty_context_database
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
