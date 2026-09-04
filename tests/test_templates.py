"""
TODO
"""

import pandas as pd
import pytest

from pydough.database_connectors.database_connector import (
    DatabaseContext,
    DatabaseDialect,
)
from pydough.errors.error_types import PyDoughMetadataException, PyDoughTypeException
from pydough.metadata.graphs.graph_metadata import GraphMetadata
from pydough.metadata.parse import parse_json_metadata_from_file
from tests.test_pydough_functions.tpch_templates import (
    template_api_simple_call,
    template_cross_collection,
    template_dataframe_collection,
    template_datetime_days,
    template_datetime_months,
    template_df_collection_df,
    template_follow_up_call,
    template_literal_1,
    template_recursive_call,
    template_simple_call,
)
from tests.testing_utilities import PyDoughPandasTest


@pytest.fixture(
    params=[
        pytest.param(
            # Test templates, simple direct call and using from_string
            PyDoughPandasTest(
                "result = customers.CALCULATE(\n"
                "   key,"
                "   n_orders=orders_filter_count(HAS(lines.WHERE(part.brand == 'Brand#45')))\n"
                ").TOP_K(5, by=(n_orders.DESC(), key.ASC()))\n",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [102361, 126886, 33445, 43090, 67108],
                        "n_orders": [13, 12, 11, 11, 11],
                    }
                ),
                "templates_simple_call",
            ),
            id="templates_simple_call",
        ),
        pytest.param(
            # Test templates, simple call using the api and from_string
            PyDoughPandasTest(
                "result = customers.CALCULATE(\n"
                "   key,"
                "   n_orders=pydough.call_template('orders_filter_count', labels={'orders_filter': 'High priority'})\n"
                ").TOP_K(5, by=(n_orders.DESC(), key.ASC()))\n",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [15859, 75160, 82531, 94393, 99070],
                        "n_orders": [21, 21, 21, 21, 21],
                    }
                ),
                "templates_simple_call_api",
            ),
            id="templates_simple_call_api",
        ),
        pytest.param(
            # Test templates, simple call using test function and directly
            PyDoughPandasTest(
                template_simple_call,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [133759, 43645, 1183, 4219, 7981],
                        "n_orders": [13, 12, 11, 11, 11],
                    }
                ),
                "templates_simple_call_func",
            ),
            id="templates_simple_call_func",
        ),
        pytest.param(
            # Test templates, simple call using test function and api
            PyDoughPandasTest(
                template_api_simple_call,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["FRANCE", "RUSSIA", "ROMANIA"],
                        "n_customers": [4149, 4089, 4087],
                    }
                ),
                "templates_simple_call_func_api",
            ),
            id="templates_simple_call_func_api",
        ),
        pytest.param(
            # Test templates, template called inside another template, both
            # directly
            PyDoughPandasTest(
                "result = orders_revenue_by(1996, customer.market_segment).WHERE("
                "(dimension == 'FURNITURE')\n"
                ")\n",
                "TPCH",
                lambda: pd.DataFrame(
                    {"dimension": ["FURNITURE"], "segment_revenue": [6.671056e09]}
                ),
                "templates_nested_call",
            ),
            id="templates_nested_call",
        ),
        pytest.param(
            # Test templates, two templates calls, first returns the input for the
            # next one. First, called through api then second one directly. In a
            # test function
            PyDoughPandasTest(
                template_follow_up_call,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "dimension": ["May", "Feb"],
                        "segment_revenue": [2.841049e09, 2.563501e09],
                        "comparison_value": [145553.014750, 145786.015957],
                    }
                ),
                "templates_follow_up_call",
            ),
            id="templates_follow_up_call",
        ),
        pytest.param(
            # Test templates, direct called and from_string, template that returns a literal
            # integer
            PyDoughPandasTest(
                "selected_customers = customers.WHERE("
                "(account_balance >= multiply_by_2(1000))\n"
                ")\n"
                "result = TPCH.CALCULATE(n_custs=COUNT(selected_customers))\n",
                "TPCH",
                lambda: pd.DataFrame({"n_custs": [109077]}),
                "templates_literal",
            ),
            id="templates_literal",
        ),
        pytest.param(
            # Test templates literals using api and calling a test function, template
            # that returns an integer
            PyDoughPandasTest(
                template_literal_1,
                "TPCH",
                lambda: pd.DataFrame({"n_custs": [81779]}),
                "templates_literal_api_func",
            ),
            id="templates_literal_api_func",
        ),
        pytest.param(
            # Test templates literals, template that generates a list of
            # literals and returns it called through the pydough API and from_string
            PyDoughPandasTest(
                "high_priority_list = pydough.call_template('order_lvl_priority', labels={'level': 'LEVEL 3'})\n"
                "medium_priority_list = pydough.call_template('order_lvl_priority', labels={'level': 'LEVEL 2'})\n"
                "low_priority_list = pydough.call_template('order_lvl_priority', labels={'level': 'LEVEL 1'})\n"
                "other_priority_list = pydough.call_template('order_lvl_priority', labels={'level': 'LEVEL 0'})\n"
                "result = TPCH.CALCULATE(\n"
                "   n_high_orders=COUNT(orders.WHERE(ISIN(order_priority, high_priority_list))),\n"
                "   n_medium_orders=COUNT(orders.WHERE(ISIN(order_priority, medium_priority_list))),\n"
                "   n_low_orders=COUNT(orders.WHERE(ISIN(order_priority, low_priority_list))),\n"
                "   n_other_orders=COUNT(orders.WHERE(ISIN(order_priority, other_priority_list)))\n"
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n_high_orders": [600434],
                        "n_medium_orders": [599312],
                        "n_low_orders": [300254],
                        "n_other_orders": [300254],
                    }
                ),
                "templates_literal_list",
            ),
            id="templates_literal_list",
        ),
        pytest.param(
            # Test templates literals, template that generates a dictionary of
            # literals and returns it
            PyDoughPandasTest(
                "result = customers.CALCULATE(\n"
                "   **customer_calculate('full_name', 'country', 'customer_balance')\n"
                ").TOP_K(5, by=customer_balance.DESC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "full_name": [
                            "Customer#000061453",
                            "Customer#000069321",
                            "Customer#000144232",
                            "Customer#000002487",
                            "Customer#000023828",
                        ],
                        "country": [
                            "MOROCCO",
                            "MOROCCO",
                            "GERMANY",
                            "UNITED STATES",
                            "MOZAMBIQUE",
                        ],
                        "customer_balance": [
                            9999.99,
                            9999.96,
                            9999.74,
                            9999.72,
                            9999.64,
                        ],
                    }
                ),
                "templates_literal_dict",
            ),
            id="templates_literal_dict",
        ),
        pytest.param(
            # Test templates datetime, receives a datetime and adds days to it using
            # pydough
            PyDoughPandasTest(
                template_datetime_days,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [2, 19008, 23686, 57953, 63589],
                        "order_date": [
                            "1996-12-01",
                            "1996-12-01",
                            "1996-12-01",
                            "1996-12-01",
                            "1996-12-01",
                        ],
                        "date_plus_days": [
                            "1996-12-11",
                            "1996-12-11",
                            "1996-12-11",
                            "1996-12-11",
                            "1996-12-11",
                        ],
                    }
                ),
                "templates_literal_datetime_days",
            ),
            id="templates_literal_datetime_days",
        ),
        pytest.param(
            # Test templates datetime, receives a datetime and adds months to it,
            # returning a datetime
            PyDoughPandasTest(
                template_datetime_months,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [4, 2532, 7075, 9127, 36610],
                        "order_date": [
                            "1995-10-11",
                            "1995-10-11",
                            "1995-10-11",
                            "1995-10-11",
                            "1995-10-11",
                        ],
                        "date_plus_months": [
                            "1996-08-11 00:00:00",
                            "1996-08-11 00:00:00",
                            "1996-08-11 00:00:00",
                            "1996-08-11 00:00:00",
                            "1996-08-11 00:00:00",
                        ],
                    }
                ),
                "templates_literal_datetime_months",
            ),
            id="templates_literal_datetime_months",
        ),
        pytest.param(
            # Test templates, template that generates pydough recursively
            PyDoughPandasTest(
                template_recursive_call,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "y_1994": [227597],
                        "y_1994_1996": [684860],
                        "y_1994_1998": [1046266],
                    }
                ),
                "templates_recursion_func",
            ),
            id="templates_recursion_func",
        ),
        pytest.param(
            # Test templates/range collection, template called directly creates
            # a range collection and returns it
            PyDoughPandasTest(
                "result = generate_range_collection(1, 5)",
                "TPCH",
                lambda: pd.DataFrame({"idx": [1, 2, 3, 4]}),
                "templates_range_collection",
            ),
            id="templates_range_collection",
        ),
        pytest.param(
            # Test templates/range collection, template called directly creates
            # a range collection and cross it with a given collection
            PyDoughPandasTest(
                template_cross_collection,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "idx": [4, 6, 8],
                        "key": [2, 3, 4],
                        "name": ["ASIA", "EUROPE", "MIDDLE EAST"],
                    }
                ),
                "templates_range_collection_cross",
            ),
            id="templates_range_collection_cross",
        ),
        pytest.param(
            # Test templates/dataframe collection, template called through the
            # api and returns a dataframe collection
            PyDoughPandasTest(
                template_dataframe_collection,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "names": ["blue", "red", "yellow", "purple"],
                        "idx": [0, 1, 2, 3],
                    }
                ),
                "templates_df_collection_api",
            ),
            id="templates_df_collection_api",
        ),
        pytest.param(
            # Test dataframe collection, the templates receives a dataframe directly
            # as input, and returns a dataframe collection, which is then used
            # in a CROSS opeartion.
            PyDoughPandasTest(
                template_df_collection_df,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_id": [1, 2, 3],
                        "customer_name": ["customer_1", "customer_2", "customer_3"],
                        "key": [1, 2, 3],
                        "clerk": [
                            "Clerk#000000951",
                            "Clerk#000000880",
                            "Clerk#000000955",
                        ],
                    }
                ),
                "templates_df_collection_df_input",
            ),
            id="templates_df_collection_df_input",
        ),
        pytest.param(
            # Test using to_table inside a template
            PyDoughPandasTest(
                "result = temporary_nations('ASIA')",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["INDIA", "INDONESIA", "JAPAN", "CHINA", "VIETNAM"],
                    }
                ),
                "templates_to_table",
            ),
            id="templates_to_table",
        ),
    ]
)
def tpch_templates_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests on templates using the TPC-H database.
    Returns an instance of PyDoughPandasTest containing information about the
    test.
    """
    return request.param


@pytest.mark.execute
def test_pipeline_e2e_tpch_templates(
    tpch_templates_test_data: PyDoughPandasTest,
    all_dialects_tpch_db_context: tuple[DatabaseContext, GraphMetadata],
):
    """
    Test executing the the template queries with TPC-H data from the original
    code generation.
    """
    db_context, graph = all_dialects_tpch_db_context

    # Skip BodoSQL, since checking all the custom tests with
    # it would take too long.
    if db_context.dialect == DatabaseDialect.BODOSQL:
        pytest.skip("Skipping tpch template test for BodoSQL.")

    tpch_templates_test_data.run_e2e_test(
        lambda _: graph,
        db_context,
        coerce_types=True,
        atol=5e-3,
    )


@pytest.mark.parametrize(
    "graph_name, error_message",
    [
        # Attr with no definitions
        pytest.param(
            "NO_TEMPLATES_DEFINITIONS",
            "graph 'NO_TEMPLATES_DEFINITIONS' must be a JSON object containing a field 'definitions' and field 'definitions' must be a JSON array",
            id="missing_definitions",
        ),
        # Attr with no options
        pytest.param(
            "NO_ATTIBUTE_OPTIONS",
            "graph 'NO_ATTIBUTE_OPTIONS' must be a JSON object containing a field 'options' and field 'options' must be a JSON array",
            id="missing_options",
        ),
    ],
)
def test_invalid_metadata_templates(
    invalid_templates_graph_path: str, graph_name: str, error_message: str
) -> None:
    with pytest.raises(
        (PyDoughMetadataException, PyDoughTypeException), match=error_message
    ):
        parse_json_metadata_from_file(
            file_path=invalid_templates_graph_path, graph_name=graph_name
        )
