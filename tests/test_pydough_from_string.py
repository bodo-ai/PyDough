"""
Tests pydough from_string API
"""

import datetime
import re
from collections.abc import Callable
from typing import Any

import pandas as pd
import pytest

import pydough
from pydough.configs import PyDoughSession
from pydough.database_connectors import DatabaseContext
from pydough.errors import PyDoughSessionException
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode
from pydough.unqualified.unqualified_transform import extract_var
from tests.testing_utilities import graph_fetcher


@pytest.mark.parametrize(
    "code, expected",
    [
        pytest.param("# fake = 2\nresult = 1", "result", id="leading_comment"),
        pytest.param(
            "result = 1  # inline_fake = 2\n# trailing_fake = 3",
            "result",
            id="inline_and_trailing_comments",
        ),
        pytest.param("left = 1\nright = 2", "right", id="last_assignment"),
        pytest.param("left = right = 1", "right", id="chained_assignment"),
        pytest.param(
            "result = 1\nif result == 1:\n    nested = 4",
            "result",
            id="nested_assignment_ignored",
        ),
        pytest.param("result: int = 3", None, id="int_annotation_ignored"),
        pytest.param("result = orders.CALCULATE(value=1)", "result", id="pydough_call"),
        pytest.param(
            "result: UnqualifiedNode = orders", "result", id="unqualified_annotation"
        ),
        pytest.param(
            "result: DataFrame = orders", None, id="dataframe_annotation_ignored"
        ),
        pytest.param("result: UnqualifiedNode", None, id="annotation_without_value"),
        pytest.param(
            "result: UnqualifiedNode | None = orders",
            None,
            id="union_annotation_ignored",
        ),
        pytest.param(
            'result: "UnqualifiedNode" = orders', None, id="string_annotation_ignored"
        ),
        pytest.param(
            "result: pydough.UnqualifiedNode = orders",
            None,
            id="qualified_annotation_ignored",
        ),
        pytest.param(
            "first = orders\nresult: UnqualifiedNode = orders",
            "result",
            id="annotated_assignment_after_plain",
        ),
        pytest.param(
            "first: UnqualifiedNode = orders\nresult = orders",
            "result",
            id="plain_assignment_after_annotated",
        ),
        pytest.param(
            "first = orders\nignored: DataFrame = orders",
            "first",
            id="trailing_other_annotation_ignored",
        ),
        pytest.param(
            "first = orders\nignored: UnqualifiedNode",
            "first",
            id="trailing_declaration_ignored",
        ),
        pytest.param(
            "if True:\n    nested: UnqualifiedNode = orders",
            None,
            id="annotated_assignment_in_block_ignored",
        ),
        pytest.param(
            "def example():\n    nested: UnqualifiedNode = orders",
            None,
            id="annotated_assignment_in_function_ignored",
        ),
        pytest.param(
            "holder.value: UnqualifiedNode = orders",
            None,
            id="annotated_attribute_target_ignored",
        ),
        pytest.param(
            "holder[0]: UnqualifiedNode = orders",
            None,
            id="annotated_subscript_target_ignored",
        ),
        pytest.param(
            '"""fake: UnqualifiedNode = orders"""',
            None,
            id="standalone_triple_quoted_string_ignored",
        ),
        pytest.param("print('hello')", None, id="standalone_call_ignored"),
        pytest.param(
            "def example():\n    nested = 42", None, id="assignment_in_function_ignored"
        ),
        pytest.param(
            "result = 1\nholder.value = 2", "result", id="trailing_attribute_ignored"
        ),
        pytest.param(
            "result = orders\n1 == 2\norders",
            "result",
            id="trailing_expressions_ignored",
        ),
        pytest.param(
            "left, right = 1, 2\nobj.attr = 3\nvalues[0] = 4\n(x := 5)",
            None,
            id="no_supported_assignment",
        ),
    ],
)
def test_extract_var(code: str, expected: str | None) -> None:
    """Test which Python assignments are eligible for result inference."""
    assert extract_var(code) == expected


def test_extract_var_propagates_syntax_error() -> None:
    """Test that invalid source raises a syntax error from the extractor."""
    with pytest.raises(SyntaxError):
        extract_var("result =")


@pytest.mark.parametrize(
    "pydough_code, answer_variable",
    [
        pytest.param(
            "result = orders\nselected = nations.CALCULATE(name)",
            None,
            id="last_assignment_even_when_result_exists",
        ),
        pytest.param(
            "from pydough.unqualified import UnqualifiedNode\n"
            "selected: UnqualifiedNode = nations.CALCULATE(name)",
            None,
            id="annotated_result",
        ),
        pytest.param(
            "if True:\n    result = nations.CALCULATE(name)",
            None,
            id="fallback_to_result",
        ),
        pytest.param(
            "selected = nations.CALCULATE(name)\nlast = 42",
            "selected",
            id="explicit_name_overrides_inference",
        ),
    ],
)
def test_from_string_result_selection(
    pydough_code: str,
    answer_variable: str | None,
    get_sample_graph: graph_fetcher,
) -> None:
    """Test which value from_string returns without executing a database query."""
    graph: GraphMetadata = get_sample_graph("TPCH")
    query: UnqualifiedNode = pydough.from_string(
        pydough_code, answer_variable, metadata=graph
    )

    assert pydough.to_sql(query, metadata=graph) == (
        "SELECT\n  n_name AS name\nFROM tpch.nation"
    )


@pytest.mark.execute
@pytest.mark.parametrize(
    "pydough_code, answer_variable, env, answer",
    [
        pytest.param(
            "nation_counts = TPCH.CALCULATE(n_nations=COUNT(nations))",
            None,
            None,
            lambda: pd.DataFrame({"n_nations": [25]}),
            id="count_nations-inferred_var-no_env",
        ),
        pytest.param(
            "answer_var = TPCH.CALCULATE(n_nations=COUNT(nations))",
            "answer_var",
            None,
            lambda: pd.DataFrame({"n_nations": [25]}),
            id="count_nations-with_var-no_env",
        ),
        pytest.param(
            "result = TPCH.CALCULATE(n_custs=COUNT(customers.WHERE(nation.name == selected_nation_name)))",
            None,
            {"selected_nation_name": "JAPAN"},
            lambda: pd.DataFrame({"n_custs": [5948]}),
            id="count_custs_in_nation-no_var-with_env",
        ),
        pytest.param(
            "automobile_customers=customers.WHERE(market_segment == SEG)\nresult = TPCH.CALCULATE(n_custs=COUNT(automobile_customers))",
            None,
            {"SEG": "AUTOMOBILE"},
            lambda: pd.DataFrame({"n_custs": [29752]}),
            id="count_custs_in_market-no_var-with_env-intermediate_result",
        ),
        pytest.param(
            "result = customers.CALCULATE(cust_name=name).orders.WHERE((order_date >= date(YEAR, 1, 1)) & (order_date < date(YEAR + 1, 1, 1))).TOP_K(1, by=total_price.DESC()).CALCULATE(cust_name=cust_name, total=total_price)",
            None,
            {"date": datetime.date, "YEAR": 1996},
            lambda: pd.DataFrame(
                {"cust_name": ["Customer#000066790"], "total": [515531.82]}
            ),
            id="customer_with_greater_order_by_year",
        ),
        pytest.param(
            """
def template(custs_filters, order_year: int):
    return (
        customers.WHERE(
            custs_filters
        ).CALCULATE(
            name,
            n_orders=COUNT(orders.WHERE(YEAR(order_date)==order_year))
        )
    )
result = template(custs_filters=ISIN(INTEGER(GETPART(name, '#', 2)), (100, 200, 300)), order_year=1993)""",
            None,
            None,
            lambda: pd.DataFrame(
                {
                    "name": [
                        "Customer#000000100",
                        "Customer#000000200",
                        "Customer#000000300",
                    ],
                    "n_orders": [3, 4, 0],
                }
            ),
            id="template_no_var-no_env",
        ),
        pytest.param(
            """
def template(custs_filters, order_year: int):
    return (
        customers.WHERE(
            custs_filters
        ).CALCULATE(
            name,
            n_orders=COUNT(orders.WHERE(YEAR(order_date)==order_year))
        )
    )
customers_result = template(custs_filters=ISIN(INTEGER(GETPART(name, '#', 2)), (100, 200, 300)), order_year=1993)""",
            "customers_result",
            None,
            lambda: pd.DataFrame(
                {
                    "name": [
                        "Customer#000000100",
                        "Customer#000000200",
                        "Customer#000000300",
                    ],
                    "n_orders": [3, 4, 0],
                }
            ),
            id="template_var-no_env",
        ),
        pytest.param(
            """
def template(order_year: int):
    return (
        customers.WHERE(
            (name==customer_name)
        ).CALCULATE(
            name,
            n_orders=COUNT(orders.WHERE(YEAR(order_date)==order_year))
        )
    )
result = template(order_year=filter_year)""",
            None,
            {"filter_year": 1992, "customer_name": "Customer#000000100"},
            lambda: pd.DataFrame(
                {
                    "name": [
                        "Customer#000000100",
                    ],
                    "n_orders": [3],
                }
            ),
            id="template_no_var-env",
        ),
        pytest.param(
            """
def template(mkt_segments: list[str]):
    return (
        regions.WHERE(
            (name==region_name)
        ).nations.CALCULATE(
            name,
            n_customers=COUNT(customers.WHERE(ISIN(market_segment, mkt_segments)))
        )
    )
cnt_customers = template(mkt_segments=segments_list)""",
            "cnt_customers",
            {"region_name": "EUROPE", "segments_list": ("BUILDING", "FURNITURE")},
            lambda: pd.DataFrame(
                {
                    "name": [
                        "FRANCE",
                        "GERMANY",
                        "ROMANIA",
                        "RUSSIA",
                        "UNITED KINGDOM",
                    ],
                    "n_customers": [2469, 2316, 2366, 2399, 2400],
                }
            ),
            id="template_var-env",
        ),
    ],
)
def test_tpch_data_e2e_from_string(
    pydough_code: str,
    answer_variable: str | None,
    env: dict[str, Any] | None,
    answer: Callable[[], pd.DataFrame],
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """Test pydough from_string API"""
    graph: GraphMetadata = get_sample_graph("TPCH")
    query: UnqualifiedNode = pydough.from_string(
        pydough_code, answer_variable, metadata=graph, environment=env
    )
    result: pd.DataFrame = pydough.to_df(
        query, metadata=graph, database=sqlite_tpch_db_context
    )
    refsol: pd.DataFrame = answer()

    # Perform the comparison between the result and the reference solution
    pd.testing.assert_frame_equal(result, refsol)


@pytest.mark.parametrize(
    "pydough_code, answer_variable, env, error_message",
    [
        pytest.param(
            "if True:\n    result2 = TPCH.CALCULATE(n_nations=COUNT(nations))",
            None,
            None,
            "PyDough code expected to store the answer in a variable named 'result'.",
            id="invalid_variable_never_defined",
        ),
        pytest.param(
            "result = TPCH.CALCULATE(n_nations=COUNT(nations))",
            "answer",
            None,
            "PyDough code expected to store the answer in a variable named 'answer'.",
            id="invalid_variable_wrong_name",
        ),
        pytest.param(
            "query = TPCH.CALCULATE(n_nations=COUNT(nations))\nresult = selected_nation_name",
            "result",
            {"selected_nation_name": "JAPAN"},
            "Expected variable 'result' in the text to store PyDough code, instead found 'str'.",
            id="invalid_pydough_result_type",
        ),
        pytest.param(
            "result = TPCH.CALCULATE(n_nations=COUNT(nations))\ncount = 1",
            None,
            None,
            "Expected variable 'count' in the text to store PyDough code, instead found 'int'.",
            id="invalid_inferred_result_type",
        ),
        pytest.param(
            "result = nations.CALCULATE(n_nations=COUNT())",
            None,
            None,
            re.escape(
                "Invalid operator invocation 'COUNT()': Expected 1 argument, received 0"
            ),
            id="invalid_pydough_code",
        ),
        pytest.param(
            "result = TPCH.CALCULATE(n_nations=COUNT(nations)",
            None,
            None,
            re.escape(
                "Syntax error in source PyDough code:\nresult = TPCH.CALCULATE(n_nations=COUNT(nations)\n'(' was never closed (<unknown>, line 1)"
            ),
            id="invalid_python_code",
        ),
    ],
)
def test_invalid_tpch_data_e2e_from_string(
    pydough_code: str,
    answer_variable: str | None,
    env: dict[str, Any] | None,
    error_message: str,
    get_sample_graph: graph_fetcher,
):
    with pytest.raises(Exception, match=error_message):
        graph: GraphMetadata = get_sample_graph("TPCH")
        query: UnqualifiedNode = pydough.from_string(
            pydough_code, answer_variable, metadata=graph, environment=env
        )
        pydough.to_sql(query, metadata=graph)


def test_from_string_session_and_metadata_conflict(
    get_sample_graph: graph_fetcher,
):
    """
    Providing both session= and metadata= to from_string is ambiguous and
    should raise PyDoughSessionException immediately.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    session = PyDoughSession()
    session.metadata = graph
    with pytest.raises(
        PyDoughSessionException,
        match="Cannot provide both 'session' and 'metadata' to from_string",
    ):
        pydough.from_string(
            "result = nations.CALCULATE(name)",
            metadata=graph,
            session=session,
        )
