"""
Error-handling unit tests the PyDough qualification process that transforms
unqualified nodes into qualified DAG nodes.
"""

import re

import pytest

import pydough
from pydough.configs import PyDoughSession
from pydough.unqualified import (
    UnqualifiedNode,
    qualify_node,
)


@pytest.mark.parametrize(
    "pydough_text, error_msg",
    [
        pytest.param(
            "result = nations.CALCULATE(nation_name=name, total_balance=SUM(account_balance))",
            "Unrecognized term of TPCH.nations: 'account_balance'. Did you mean: name, comment, key, region, customers?",
            id="bad_name",
        ),
        pytest.param(
            "result = nations.CALCULATE(nation_name=FIZZBUZZ(name))",
            "PyDough object FIZZBUZZ is not callable. Did you mean: FIND, ABS, MIN, SUM, HOUR?",
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
            "PyDough object SUB is not callable. Did you mean: SUM, STD, ABS, AVG, DAY?",
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
            "Error while parsing 'per' string of RANKING(by=(key.ASC(na_pos='first'), per='custs') in context TPCH.customers.orders (unrecognized ancestor 'custs'; did you mean one of: 'TPCH', 'customers')",
            id="bad_per_1",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:2'))",
            "Error while parsing 'per' string of RANKING(by=(key.ASC(na_pos='first'), per='customers:2') in context TPCH.customers.orders (there are not 2 ancestors of the current context with name 'customers')",
            id="bad_per_2",
        ),
        pytest.param(
            "result = customers.orders.customer.orders.lines.CALCULATE(RANKING(by=extended_price.DESC(), per='orders'))",
            "Error while parsing 'per' string of RANKING(by=(extended_price.DESC(na_pos='last'), per='orders') in context TPCH.customers.orders.customer.orders.lines (per-string 'orders' is ambiguous in this context; use the form 'orders:index' to disambiguate, where 'orders:1' refers to the most recent ancestor)",
            id="bad_per_3",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:k'))",
            "Error while parsing 'per' string of RANKING(by=(key.ASC(na_pos='first'), per='customers:k') in context TPCH.customers.orders (expected the index after ':' to be a positive integer)",
            id="bad_per_4",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:1:2'))",
            "Error while parsing 'per' string of RANKING(by=(key.ASC(na_pos='first'), per='customers:1:2') in context TPCH.customers.orders (expected 0 or 1 ':', found 2))",
            id="bad_per_5",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:'))",
            "Error while parsing 'per' string of RANKING(by=(key.ASC(na_pos='first'), per='customers:') in context TPCH.customers.orders (expected the index after ':' to be a positive integer)",
            id="bad_per_6",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per='customers:0'))",
            "Error while parsing 'per' string of RANKING(by=(key.ASC(na_pos='first'), per='customers:0') in context TPCH.customers.orders (expected the index after ':' to be a positive integer)",
            id="bad_per_7",
        ),
        pytest.param(
            "result = customers.orders.CALCULATE(RANKING(by=key.ASC(), per=-1))",
            "`per` argument must be a string",
            id="bad_per_8",
        ),
        pytest.param(
            "foo = TPCH.regions.nations.customers\n"
            "bar = TPCH.nations.customers.orders.lines.supplier.nation.region.nations.customers\n"
            "result = foo.CROSS(bar).CALCULATE(r=RANKING(by=name.ASC(), per='TPCH'))",
            "Error while parsing 'per' string of RANKING(by=(name.ASC(na_pos='first'), per='TPCH') in context TPCH.regions.nations.customers.TPCH.nations.customers.orders.lines.supplier.nation.region.nations.customers (per-string 'TPCH' is ambiguous in this context; use the form 'TPCH:index' to disambiguate, where 'TPCH:1' refers to the most recent ancestor)",
            id="bad_per_9",
        ),
        pytest.param(
            "foo = TPCH.regions.nations.customers\n"
            "bar = TPCH.nations.customers.orders.lines.supplier.nation.region.nations.customers\n"
            "result = foo.CROSS(bar).CALCULATE(r=RANKING(by=name.ASC(), per='fizz'))",
            "Error while parsing 'per' string of RANKING(by=(name.ASC(na_pos='first'), per='fizz') in context TPCH.regions.nations.customers.TPCH.nations.customers.orders.lines.supplier.nation.region.nations.customers (unrecognized ancestor 'fizz'; did you mean one of: 'TPCH:2', 'regions', 'nations:3', 'customers:2', 'TPCH:1', 'nations:2', 'customers:1', 'orders', 'lines', 'supplier', 'nation', 'region', 'nations:1')",
            id="bad_per_10",
        ),
        pytest.param(
            "result = nations.CALCULATE(name=name, var=SAMPLE_VAR(suppliers.account_balance))",
            "PyDough object SAMPLE_VAR is not callable. Did you mean: YEAR, SUM, UPPER, VAR, AVG?",
            id="kwargfunc_1",
        ),
        pytest.param(
            "result = nations.CALCULATE(name=name, var=SAMPLE_VAR(suppliers.account_balance))",
            "PyDough object SAMPLE_VAR is not callable. Did you mean: YEAR, SUM, UPPER, VAR, AVG?",
            id="kwargfunc_2",
        ),
        pytest.param(
            "result = nations.CALCULATE(name=name, var=SAMPLE_STD(suppliers.account_balance))",
            "PyDough object SAMPLE_STD is not callable. Did you mean: SMALLEST, STD, HAS, LARGEST, SUM?",
            id="kwargfunc_3",
        ),
        pytest.param(
            "result = nations.CALCULATE(name=name, std=POPULATION_STD(suppliers.account_balance))",
            "PyDough object POPULATION_STD is not callable. Did you mean: COUNT, ROUND, CONTAINS, FIND, LPAD?",
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
        pytest.param(
            "result = TPCH.CALCULATE(x=COUNT(TPCH.nations) / COUNT(TPCH.regions))",
            "Unrecognized term of TPCH: 'TPCH'. Did you mean: lines, parts, orders, nations, regions?",
            id="double_graph",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v')",
            "Must provide `index_name` to EXPLODE when `is_distinct` is False",
            id="bad_explode_01",
        ),
        pytest.param(
            "result = nations.EXPLODE()",
            "missing 3 required positional arguments: 'data', 'name', and 'value_name'",
            id="bad_explode_02",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names')",
            "missing 1 required positional argument: 'value_name'",
            id="bad_explode_03",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, value_name='v')",
            "missing 1 required positional argument: 'name'",
            id="bad_explode_04",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='foo')",
            "Unrecognized `version` for EXPLODE: 'foo' (must be either 'array' or 'string')",
            id="bad_explode_05",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='string')",
            "Must provide a string `delimiter` to EXPLODE when `version` is 'string'",
            id="bad_explode_06",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='string', delimiter=['x'])",
            "Must provide a string `delimiter` to EXPLODE when `version` is 'string'",
            id="bad_explode_07",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='string', delimiter=42)",
            "Must provide a string `delimiter` to EXPLODE when `version` is 'string'",
            id="bad_explode_08",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', delimiter='x')",
            "Cannot provide a `delimiter` to EXPLODE when `version` is 'array'",
            id="bad_explode_09",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='array', delimiter='x')",
            "Cannot provide a `delimiter` to EXPLODE when `version` is 'array'",
            id="bad_explode_10",
        ),
        pytest.param(
            "result = nations.EXPLODE(nation_names, 'names', value_name='v', index_name='i')",
            "Unrecognized term of TPCH.nations: 'nation_names'. Did you mean: name, region, region_key?",
            id="bad_explode_11",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='string', delimiter=0)",
            "Must provide a string `delimiter` to EXPLODE when `version` is 'string'",
            id="bad_explode_12",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 0, value_name='v', index_name='i', version='string', delimiter=' ')",
            "Invalid `name` argument for EXPLODE: 0",
            id="bad_explode_13",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name=0, index_name='i', version='string', delimiter=' ')",
            "Invalid `value_name` argument for EXPLODE: 0",
            id="bad_explode_14",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name=0, version='string', delimiter=' ')",
            "Invalid `index_name` argument for EXPLODE: 0",
            id="bad_explode_15",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version=0, delimiter=' ')",
            "Unrecognized `version` for EXPLODE: 0 (must be either 'array' or 'string')",
            id="bad_explode_16",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='string', delimiter=' ', filtering='yay')",
            "Invalid `filtering` argument for EXPLODE: 'yay'",
            id="bad_explode_17",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='string', delimiter=' ', is_distinct='nope')",
            "Invalid `is_distinct` argument for EXPLODE: 'nope'",
            id="bad_explode_18",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='i', version='string', delimiter=' ', extra_kwarg='fizzbuzz')",
            "UnqualifiedNode.EXPLODE() got an unexpected keyword argument 'extra_kwarg'",
            id="bad_explode_19",
        ),
        pytest.param(
            "result = nations.EXPLODE(customers.name, 'names', value_name='v', index_name='i', version='string', delimiter='#')",
            "Invalid data argument to explode: customers.name (explode does not currently support exploding data from a child collection; make sure to store the data to be exploded in a column of the parent collection before calling explode)",
            id="bad_explode_20",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='name', index_name='i', version='string', delimiter='#')",
            "Cannot use 'name' as the `value_name` for EXPLODE because it is already a term in the ancestor context",
            id="bad_explode_21",
        ),
        pytest.param(
            "result = nations.EXPLODE(name, 'names', value_name='v', index_name='key', version='string', delimiter='#')",
            "Cannot use 'key' as the `index_name` for EXPLODE because it is already a term in the ancestor context",
            id="bad_explode_22",
        ),
    ],
)
def test_qualify_error(
    pydough_text: str,
    error_msg: str,
    empty_sqlite_tpch_session: PyDoughSession,
) -> None:
    """
    Tests that the qualification process correctly raises the expected error
    messages when the PyDough text is invalid. Takes in the PyDough text and
    converts it to unqualified nodes with `from_string`, then qualifies it to
    ensure that the error is raised as expected. The PyDough text can be 1 or
    multiple lines, but must end with storing the answers in a variable
    called `result`.
    """
    with pytest.raises(Exception, match=re.escape(error_msg)):
        unqualified: UnqualifiedNode = pydough.from_string(
            pydough_text,
            answer_variable="result",
            metadata=empty_sqlite_tpch_session.metadata,
        )
        qualify_node(unqualified, empty_sqlite_tpch_session)
