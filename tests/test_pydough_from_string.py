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
from pydough.database_connectors import DatabaseContext
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode
from tests.testing_utilities import graph_fetcher


@pytest.mark.execute
@pytest.mark.parametrize(
    "pydough_code, answer_variable, env, answer",
    [
        pytest.param(
            "result = TPCH.CALCULATE(n_nations=COUNT(nations))",
            None,
            None,
            lambda: pd.DataFrame({"n_nations": [25]}),
            id="count_nations-no_var-no_env",
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
    ],
)
def test_tpch_data_e2e_from_string(
    pydough_code: str,
    answer_variable: str,
    env: dict[str, Any],
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
            "result2 = TPCH.CALCULATE(n_nations=COUNT(nations))",
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
                "Syntax error in source PyDough code:\n'(' was never closed (<unknown>, line 1)"
            ),
            id="invalid_python_code",
        ),
    ],
)
def test_invalid_tpch_data_e2e_from_string(
    pydough_code: str,
    answer_variable: str,
    env: dict[str, Any],
    error_message: str,
    get_sample_graph: graph_fetcher,
):
    with pytest.raises(Exception, match=error_message):
        graph: GraphMetadata = get_sample_graph("TPCH")
        query: UnqualifiedNode = pydough.from_string(
            pydough_code, answer_variable, metadata=graph, environment=env
        )
        pydough.to_sql(query, metadata=graph)
