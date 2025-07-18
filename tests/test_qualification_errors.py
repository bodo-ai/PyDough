"""
Error-handling unit tests the PyDough qualification process that transforms
unqualified nodes into qualified DAG nodes.
"""

import re

import pytest

import pydough
from pydough.configs import PyDoughConfigs
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
    qualify_node,
)
from tests.testing_utilities import (
    graph_fetcher,
)


@pytest.mark.parametrize(
    "pydough_text, error_msg",
    [
        pytest.param(
            "result = nations.CALCULATE(nation_name=name, total_balance=SUM(account_balance))",
            "Unrecognized term of TPCH.nations: 'account_balance'. Did you mean: comment, customers, name, region_key, suppliers, region?",
            id="bad_name",
        ),
        pytest.param(
            "result = nations.CALCULATE(nation_name=FIZZBUZZ(name))",
            "PyDough nodes FIZZBUZZ is not callable. Did you mean to use a function?",
            id="non_function",
        ),
        pytest.param(
            "result = nations.CALCULATE(y=suppliers.CALCULATE(x=COUNT(supply_records)).x)",
            "Expected all terms in CALCULATE(y=suppliers.CALCULATE(x=COUNT(supply_records)).x) to be singular, but encountered a plural expression: suppliers.CALCULATE(x=COUNT(supply_records)).x",
            id="bad_plural_1",
        ),
        pytest.param(
            "result = TPCH.nations.name.hello",
            "Expected a collection, but received an expression: TPCH.nations.name",
            id="expression_instead_of_collection",
        ),
        pytest.param(
            "result = customers.CALCULATE(r=nation.region)",
            "Expected an expression, but received a collection: nation.region",
            id="collection_instead_of_expression",
        ),
        pytest.param(
            "result = suppliers.supply_records.CALCULATE(o=lines.order.order_date)",
            "Expected all terms in CALCULATE(o=lines.order.order_date) to be singular, but encountered a plural expression: lines.order.order_date",
            id="bad_plural_2",
        ),
        pytest.param(
            "lines.CALCULATE(v=MUL(extended_price, SUB(1, discount)))",
            "PyDough nodes SUB is not callable. Did you mean to use a function?",
            id="binop_function_call",
        ),
        pytest.param(
            "TPCH.lines.tax = 0",
            "PyDough objects do not yet support writing properties to them.",
            id="setattr",
        ),
        pytest.param(
            "best_customer = nations.customers.BEST(per='nations', by=account_balance.DESC())\n"
            "result = regions.CALCULATE(n=best_customer.name)",
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=1, allow_ties=False) == 1).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=1, allow_ties=False) == 1).name",
            id="bad_best_1",
        ),
        pytest.param(
            "best_customer = nations.customers.BEST(per='regions', by=account_balance.DESC(), allow_ties=True)\n"
            "result = regions.CALCULATE(n=best_customer.name)",
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name",
            id="bad_best_2",
        ),
        pytest.param(
            "best_customer = nations.customers.BEST(per='regions', by=account_balance.DESC(), n_best=3)\n"
            "result = regions.CALCULATE(n=best_customer.name)",
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=False) <= 3).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=False) <= 3).name",
            id="bad_best_3",
        ),
        pytest.param(
            "result = regions.nations.customers.BEST(per='regions', by=account_balance.DESC(), n_best=3, allow_ties=True)",
            "Cannot allow ties when multiple best values are requested",
            id="bad_best_4",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='custs'))",
            "Per string refers to unrecognized ancestor 'custs' of TPCH.customers.orders",
            id="bad_per_1",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:2'))",
            "Per string 'customers:2' invalid as there are not 2 ancestors of the current context with name 'customers'.",
            id="bad_per_2",
        ),
        pytest.param(
            "result = customers.orders.customer.orders.lines.CALCULATE(RANKING(by=extended_price.DESC(), per='orders'))",
            "Per string 'orders' is ambiguous for TPCH.customers.orders.customer.orders.lines. Use the form 'orders:index' to disambiguate, where 'orders:1' refers to the most recent ancestor.",
            id="bad_per_3",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:k'))",
            "Malformed per string: 'customers:k' (expected the index after ':' to be a positive integer)",
            id="bad_per_4",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:1:2'))",
            "Malformed per string: 'customers:1:2' (expected 0 or 1 ':', found 2)",
            id="bad_per_5",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:'))",
            "Malformed per string: 'customers:' (expected the index after ':' to be a positive integer)",
            id="bad_per_6",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:0'))",
            "Malformed per string: 'customers:0' (expected the index after ':' to be a positive integer)",
            id="bad_per_7",
        ),
        pytest.param(
            "result = nations.CALCULATE(name=name, var=SAMPLE_VAR(suppliers.account_balance))",
            "PyDough nodes SAMPLE_VAR is not callable. Did you mean to use a function?",
            id="kwargfunc_1",
        ),
        pytest.param(
            "result = nations.CALCULATE(name=name, var=SAMPLE_VARIANCE(suppliers.account_balance))",
            "PyDough nodes SAMPLE_VARIANCE is not callable. Did you mean to use a function?",
            id="kwargfunc_2",
        ),
        pytest.param(
            "result = nations.CALCULATE(name=name, var=SAMPLE_STD(suppliers.account_balance))",
            "PyDough nodes SAMPLE_STD is not callable. Did you mean to use a function?",
            id="kwargfunc_3",
        ),
        pytest.param(
            "result = nations.CALCULATE(name=name, std=POPULATION_STD(suppliers.account_balance))",
            "PyDough nodes POPULATION_STD is not callable. Did you mean to use a function?",
            id="kwargfunc_4",
        ),
        pytest.param(
            "result = nations.CALCULATE(name).customers.CALCULATE(name)",
            "Unclear whether 'name' refers to a term of the current context or ancestor of collection TPCH.nations.CALCULATE(name=name).customers",
            id="downstream_1",
        ),
        pytest.param(
            "result = regions.CALCULATE(name).nations.customers.CALCULATE(name)",
            "Unclear whether 'name' refers to a term of the current context or ancestor of collection TPCH.regions.CALCULATE(name=name).nations.customers",
            id="downstream_2",
        ),
        pytest.param(
            "result = orders.PARTITION(name='priorities', by=order_priority).CALCULATE(key=COUNT(orders)).orders.CALCULATE(key)",
            "Unclear whether 'key' refers to a term of the current context or ancestor of collection TPCH.Partition(orders, name='priorities', by=order_priority).CALCULATE(key=COUNT(orders)).orders",
            id="downstream_3",
        ),
        pytest.param(
            "result = regions.CALCULATE(n1=name, n2=CROSS(regions).name)",
            "Expected all terms in CALCULATE(n1=name, n2=TPCH.regions.name) to be singular, but encountered a plural expression: TPCH.regions.name",
            id="plural_cross",
        ),
        pytest.param(
            "result = nations.CALCULATE(replace_name1=REPLACE(name, 'a', 'b', 'c'))",
            "Expected between 2 and 3 arguments inclusive, received 4",
            id="bad_replace_too_many_args",
        ),
        pytest.param(
            "result = nations.CALCULATE(replace_name2=REPLACE('a'))",
            "Expected between 2 and 3 arguments inclusive, received 1",
            id="bad_replace_few_args",
        ),
        pytest.param(
            "result = nations.CALCULATE(str_count1=STRCOUNT(name, 'a', 'b'))",
            "Expected 2 arguments, received 3",
            id="bad_str_count_too_many_args",
        ),
        pytest.param(
            "result = nations.CALCULATE(str_count2=STRCOUNT(name))",
            "Expected 2 arguments, received 1",
            id="bad_str_count_few_args",
        ),
    ],
)
def test_qualify_error(
    pydough_text: str,
    error_msg: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that the qualification process correctly raises the expected error
    messages when the PyDough text is invalid. Takes in the PyDough text and
    converts it to unqualified nodes with `from_string`, then qualifies it to
    ensure that the error is raised as expected. The PyDough text can be 1 or
    multiple lines, but must end with storing the answers in a variable
    called `result`.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    default_config: PyDoughConfigs = pydough.active_session.config
    with pytest.raises(Exception, match=re.escape(error_msg)):
        unqualified: UnqualifiedNode = pydough.from_string(
            pydough_text, answer_variable="result", metadata=graph
        )
        qualify_node(unqualified, graph, default_config)
