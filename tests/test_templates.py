"""
TODO
"""

import re

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
from tests.testing_utilities import (
    PyDoughPandasTest,
    graph_fetcher,
    run_e2e_error_test,
)


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


@pytest.mark.execute
@pytest.mark.parametrize(
    "pydough_impl, columns, error_message",
    [
        pytest.param(
            "result = orders_filter_count2()",
            None,
            "PyDough object orders_filter_count2 is not callable. Did you mean: orders_filter_count, RELCOUNT, STRCOUNT?",
            id="unexisting_template_definition_call",
        ),
        pytest.param(
            "result = pydough.call_template('orders_filter_count2', labels={})",
            None,
            "PyDough template 'orders_filter_count2' doesn't exist. Did you mean: orders_filter_count, order_revenue, order_lvl_priority?",
            id="unexisting_template_definition_api",
        ),
        pytest.param(
            "result = orders_filter_count(no_param=True)",
            None,
            re.escape(
                "orders_filter_count() got an unexpected keyword argument 'no_param'"
            ),
            id="wrong_template_arguments_call",
        ),
        pytest.param(
            "result = pydough.call_template('orders_filter_count', labels={'no_param': 'LABEL 1'})",
            None,
            "Template 'orders_filter_count' doesn't have a paramater called 'no_param'. Did you mean: orders_filter",
            id="wrong_template_arguments_api",
        ),
        pytest.param(
            "result = pydough.call_template('orders_filter_count', labels={'orders_filter': 'INVALID LABEL'})",
            None,
            "Label 'INVALID LABEL' not found in any attribute's options",
            id="wrong_template_label_api",
        ),
        pytest.param(
            "result = pydough.call_template('orders_filter_count', labels={'orders_filter': 'LEVEL 0'})",
            None,
            "The label 'LEVEL 0' is not available for parameter 'orders_filter' on template 'orders_filter_count'",
            id="restricted_template_attribute_api",
        ),
        pytest.param(
            "result = pydough.call_template('orders_revenue_by', labels={'arg_year': 'Month', 'arg_dimension': 'Year 1992'})",
            None,
            "The label 'Month' is not available for parameter 'arg_year' on template 'orders_revenue_by'",
            id="restricted_template_parameter_api",
        ),
    ],
)
def test_pipeline_e2e_tpch_templates_errors(
    pydough_impl: str,
    columns: dict[str, str] | list[str] | None,
    error_message: str,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Tests running bad PyDough code through the entire pipeline to verify that
    a certain error is raised.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    run_e2e_error_test(
        pydough_impl,
        error_message,
        graph,
        columns=columns,
        database=sqlite_tpch_db_context,
    )


@pytest.mark.parametrize(
    "graph_name, error_message",
    [
        # Attr with invalid name
        pytest.param(
            "INVALID_ATTRIBUTE_NAME",
            "metadata for template attribute within graph 'INVALID_ATTRIBUTE_NAME' must be a JSON object containing a field 'name' and field 'name' must be a string",
            id="invalid_attribute_name",
        ),
        # Dupplicated attribute name
        pytest.param(
            "DUPPLICATED_ATTRIBUTE_NAME",
            "Already added template attribute 'attr1' in graph 'DUPPLICATED_ATTRIBUTE_NAME'",
            id="duplicated_attribute_name",
        ),
        # Attr with invalid usage
        pytest.param(
            "INVALID_ATTRIBUTE_USAGE",
            "template attribute 'attr1' in graph 'INVALID_ATTRIBUTE_USAGE' must be a dictionary where each key must be a string and each value must be a list where each element must be a string",
            id="invalid_attribute_usage",
        ),
        # Attr with invalid type
        pytest.param(
            "INVALID_ATTRIBUTE_TYPE",
            re.escape(
                "Invalid type 'invalid_type' for attribute 'attr1' in graph 'INVALID_ATTRIBUTE_TYPE'. Must be one of: ['datetime', 'dict', 'float', 'int', 'list', 'pd.DataFrame', 'pydough', 'str']"
            ),
            id="invalid_attribute_type",
        ),
        # Attribute with no options
        pytest.param(
            "NO_ATTRIBUTE_OPTIONS",
            "Template attribute 'attr1' in graph 'NO_ATTRIBUTE_OPTIONS' must have at least one option defined.",
            id="missing_options",
        ),
        # Attribute with invalid option label
        pytest.param(
            "INVALID_ATTRIBUTE_OPTION_LABEL",
            "Option in attribute 'attr1' in graph 'INVALID_ATTRIBUTE_OPTION_LABEL' must be a JSON object containing a field 'label' and field 'label' must be a string",
            id="invalid_option_label",
        ),
        # Attribute with invalid option value
        pytest.param(
            "INVALID_ATTRIBUTE_OPTION_VALUE",
            re.escape(
                "Option in attribute 'attr1' in graph 'INVALID_ATTRIBUTE_OPTION_VALUE' 'value' fields must be either all strings or all integers (not a mix, and no other type)."
            ),
            id="invalid_option_value",
        ),
        # Duplicated option label
        pytest.param(
            "DUPPLICATED_ATTRIBUTE_OPTION_LABEL",
            "Duplicate option label: 'LABEL 1' for attribute 'attr1'. The label is already in use by attribute 'attr1' in graph 'DUPPLICATED_ATTRIBUTE_OPTION_LABEL'.",
            id="duplicated_option_label",
        ),
        # Duplicates label option across attributes
        pytest.param(
            "DUPPLICATED_ATTRIBUTE_OPTION_LABEL_2",
            "Duplicate option label: 'LABEL 1' for attribute 'attr2'. The label is already in use by attribute 'attr1' in graph 'DUPPLICATED_ATTRIBUTE_OPTION_LABEL_2'.",
            id="duplicated_option_label_across",
        ),
        # Mixed option value type
        pytest.param(
            "MIXED_TYPE_ATTRIBUTE_OPTIONS",
            re.escape(
                "Option in attribute 'attr1' in graph 'MIXED_TYPE_ATTRIBUTE_OPTIONS' 'value' fields must be either all strings or all integers (not a mix, and no other type)."
            ),
            id="mixed_type_options",
        ),
        # Attr with no definitions
        pytest.param(
            "NO_TEMPLATES_DEFINITIONS",
            "graph 'NO_TEMPLATES_DEFINITIONS' must be a JSON object containing a field 'definitions' and field 'definitions' must be a JSON array",
            id="missing_definitions",
        ),
        # Template with invalid name
        pytest.param(
            "TEMPLATE_INVALID_NAME",
            "Template definition 'name with space' in graph 'TEMPLATE_INVALID_NAME' must be a string that is a valid Python identifier",
            id="invalid_template_name",
        ),
        # Template with invalid name reserved python word
        pytest.param(
            "TEMPLATE_NAME_PYTHON_RESERVED_WORD",
            "Template definition 'continue' in graph 'TEMPLATE_NAME_PYTHON_RESERVED_WORD' must be a string that is not a Python reserved word or built-in name",
            id="invalid_template_name_python_word",
        ),
        # Template with invalid name reserved pydough word
        pytest.param(
            "TEMPLATE_NAME_PYDOUGH_RESERVED_WORD",
            "Template definition 'CALCULATE' in graph 'TEMPLATE_NAME_PYDOUGH_RESERVED_WORD' must be a string that is not a PyDough reserved word",
            id="invalid_template_name_pydough_word",
        ),
        # Duplicated template name
        pytest.param(
            "TEMPLATE_NAME_DUPLICATED",
            "Already added 'template_1' to graph 'TEMPLATE_NAME_DUPLICATED'",
            id="invalid_template_name_duplicated",
        ),
        # Template with invalid paramter name
        pytest.param(
            "TEMPLATE_INVALID_PARAMETER_NAME",
            re.escape(
                "Parameter 'invalid_#@param' in template 'template_1' in graph 'TEMPLATE_INVALID_PARAMETER_NAME' must be a string that is a valid Python identifier"
            ),
            id="invalid_template_param_name",
        ),
        # Template with invalid parameter type
        pytest.param(
            "TEMPLATE_INVALID_PARAMETER_TYPE",
            re.escape(
                "Invalid type 'invalid_type' for the parameter 'parameter_1' of template 'template_1' in graph 'TEMPLATE_INVALID_PARAMETER_TYPE'. Must be one of: ['datetime', 'dict', 'float', 'int', 'list', 'pd.DataFrame', 'pydough', 'str']"
            ),
            id="invalid_template_param_type",
        ),
        # No descrition on param
        pytest.param(
            "TEMPLATE_PARAM_NO_DESC",
            re.escape(
                "All parameters must have description in 'template_1' must be a JSON object containing a field 'description' and field 'description' must be a string"
            ),
            id="invalid_template_param_no_desc",
        ),
        # Invalid answer variable
        pytest.param(
            "TEMPLATE_INVALID_ANSWER_VAR",
            re.escape(
                "Answer variable '(no_valid_identifier)' of template 'template_1' in graph 'TEMPLATE_INVALID_ANSWER_VAR' must be a string that is a valid Python identifier"
            ),
            id="invalid_template_answer_var",
        ),
        pytest.param(
            "TEMPLATE_INVALID_SOURCE_CODE",
            re.escape(
                "Template definition 'template_1' does not contain valid Python code: invalid syntax (<unknown>, line 2)"
            ),
            id="invalid_template_source",
        ),
        pytest.param(
            "TEMPLATE_INVALID_SOURCE_CODE_2",
            re.escape(
                "Internal error: failed to compile transformed template for 'template_1': no binding for nonlocal 'foo' found (<template_1>, line 7)"
            ),
            id="invalid_template_source_2",
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
