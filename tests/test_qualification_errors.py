"""
Error-handling unit tests the PyDough qualification process that transforms
unqualified nodes into qualified DAG nodes.
"""

import re
from collections.abc import Callable

import pytest

import pydough
from pydough.configs import PyDoughConfigs
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)
from tests.testing_utilities import (
    graph_fetcher,
)


def bad_pydough_impl_01(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.nations.CALCULATE(nation_name=name, total_balance=SUM(account_balance))
    ```
    The problem: there is no property `account_balance` to be accessed from nations.
    """
    return root.nations.CALCULATE(
        nation_name=root.name, total_balance=root.SUM(root.account_balance)
    )


def bad_pydough_impl_02(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.nations.CALCULATE(nation_name=FIZZBUZZ(name))
    ```
    The problem: there is no function named FIZZBUZZ, so this looks like a
    CALCULATE being done onto a subcollection, which cannot be used as an
    expression inside a CALCULATE.
    """
    return root.nations.CALCULATE(nation_name=root.FIZZBUZZ(root.name))


def bad_pydough_impl_03(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.nations.CALCULATE(y=suppliers.CALCULATE(x=COUNT(supply_records)).x)
    ```
    The problem: `suppliers.CALCULATE(x=COUNT(supply_records))` is plural with regards
    to nations, so accessing its `x` property is still plural, therefore it
    cannot be used as a term inside a CALCULATE from the context of nations.
    """
    return root.nations.CALCULATE(
        y=root.suppliers.CALCULATE(x=root.COUNT(root.supply_records)).x
    )


def bad_pydough_impl_04(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.nations.name.hello
    ```
    The problem: nations.name is an expression, so invoking `.hello` on it is
    not valid.
    """
    return root.nations.name.hello


def bad_pydough_impl_05(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Customer(r=nation.region)
    ```
    The problem: nation.region is a collection, therefore cannot be used as
    an expression in a CALCULATE.
    """
    return root.customers.CALCULATE(r=root.nation.region)


def bad_pydough_impl_06(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.suppliers.supply_records.CALCULATE(o=lines.order.order_date)
    ```
    The problem: lines is plural with regards to supply_records, therefore
    lines.order.order_date is also plural and it cannot be used in a CALCULATE
    in the context of supply_records.
    """
    return root.suppliers.supply_records.CALCULATE(o=root.lines.order.order_date)


def bad_pydough_impl_07(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.lines.CALCULATE(v=MUL(extended_price, SUB(1, discount)))
    ```
    The problem: there is no function named MUL or SUB, so this looks like a
    CALCULATE operation on a subcollection, which cannot be used as an
    expression inside of a CALCULATE.
    """
    return root.lines.CALCULATE(
        v=root.MUL(root.extended_price, root.SUB(1, root.discount))
    )


def bad_pydough_impl_08(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.lines.tax = 0
    TPCH.lines.CALCULATE(value=extended_price * tax)
    ```
    The problem: writing to an unqualified node is not yet supported.
    """
    root.lines.tax = 0
    return root.lines.CALCULATE(value=root.extended_price * root.tax)


def bad_pydough_impl_09(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    best_customer = nations.customers.BEST(per='nations', by=account_balance.DESC())
    regions.CALCULATE(n=best_customer.name)
    ```
    The problem: The cardinality is off since even though the `BEST` ensures
    the customers are singular with regards to the nation, the nations are
    still plural with regards to the region.
    """
    best_customer = root.nations.customers.BEST(
        per="nations", by=root.account_balance.DESC()
    )
    return root.regions.CALCULATE(n=best_customer.name)


def bad_pydough_impl_10(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    best_customer = nations.customers.BEST(per='regions', by=account_balance.DESC(), allow_ties=True)
    regions.CALCULATE(n=best_customer.name)
    ```
    The problem: the presence of `allow_ties=True` means that the `BEST`
    operator does not guarantee `nations.customers` is plural with regards to
    `regions`.
    """
    best_customer = root.nations.customers.BEST(
        per="regions", by=root.account_balance.DESC(), allow_ties=True
    )
    return root.regions.CALCULATE(n=best_customer.name)


def bad_pydough_impl_11(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    best_customer = nations.customers.BEST(per='regions', by=account_balance.DESC(), n_best=3)
    regions.CALCULATE(n=best_customer.name)
    ```
    The problem: the presence of `n_best=3` means that the `BEST` operator
    does not guarantee `nations.customers` is plural with regards to `regions`.
    """
    best_customer = root.nations.customers.BEST(
        per="regions", by=root.account_balance.DESC(), allow_ties=True
    )
    return root.regions.CALCULATE(n=best_customer.name)


def bad_pydough_impl_12(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    regions.nations.customers.BEST(per='regions', by=account_balance.DESC(), n_best=3, allow_ties=True)
    ```
    The problem: cannot simultaneously use `n_best=3` and `allow_ties=True`.
    """
    return root.regions.nations.customers.BEST(
        per="regions", by=root.account_balance.DESC(), n_best=3, allow_ties=True
    )


def bad_pydough_impl_13(root: UnqualifiedNode) -> UnqualifiedNode:
    # Non-existent per name
    return root.customers.orders.CALCULATE(root.RANKING(by=root.key.ASC(), per="custs"))


def bad_pydough_impl_14(root: UnqualifiedNode) -> UnqualifiedNode:
    # Bad index of valid per name
    return root.customers.orders.CALCULATE(
        root.RANKING(by=root.key.ASC(), per="customers:2")
    )


def bad_pydough_impl_15(root: UnqualifiedNode) -> UnqualifiedNode:
    # Ambiguous per name
    return root.customers.orders.customer.orders.lines.CALCULATE(
        root.RANKING(by=root.extended_price.DESC(), per="orders")
    )


def bad_pydough_impl_16(root: UnqualifiedNode) -> UnqualifiedNode:
    # Malformed per name
    return root.customers.orders.CALCULATE(
        root.RANKING(by=root.key.ASC(), per="customers:k")
    )


def bad_pydough_impl_17(root: UnqualifiedNode) -> UnqualifiedNode:
    # Malformed per name
    return root.customers.orders.CALCULATE(
        root.RANKING(by=root.key.ASC(), per="customers:1:2")
    )


def bad_pydough_impl_18(root: UnqualifiedNode) -> UnqualifiedNode:
    # Malformed per name
    return root.customers.orders.CALCULATE(
        root.RANKING(by=root.key.ASC(), per="customers:")
    )


def bad_pydough_impl_19(root: UnqualifiedNode) -> UnqualifiedNode:
    # Malformed per name
    return root.customers.orders.CALCULATE(
        root.RANKING(by=root.key.ASC(), per="customers:0")
    )


def bad_pydough_impl_20(root: UnqualifiedNode) -> UnqualifiedNode:
    # Internal function name
    return root.Nations.CALCULATE(
        name=root.name,
        var=root.SAMPLE_VAR(root.suppliers.account_balance),
    )


def bad_pydough_impl_21(root: UnqualifiedNode) -> UnqualifiedNode:
    # Internal function name
    return root.Nations.CALCULATE(
        name=root.name,
        var=root.SAMPLE_VARIANCE(root.suppliers.account_balance),
    )


def bad_pydough_impl_22(root: UnqualifiedNode) -> UnqualifiedNode:
    # Internal function name
    return root.Nations.CALCULATE(
        name=root.name,
        var=root.SAMPLE_STD(root.suppliers.account_balance),
    )


def bad_pydough_impl_23(root: UnqualifiedNode) -> UnqualifiedNode:
    # Internal function name
    return root.Nations.CALCULATE(
        name=root.name,
        std=root.POPULATION_STD(root.suppliers.account_balance),
    )


def bad_replace_too_many_args(root: UnqualifiedNode) -> UnqualifiedNode:
    # Too many arguments to replace
    return root.nations.CALCULATE(
        replace_name1=root.REPLACE(root.name, "a", "b", "c"),
    )


def bad_replace_few_args(root: UnqualifiedNode) -> UnqualifiedNode:
    # Not enough arguments to replace
    return root.nations.CALCULATE(replace_name2=root.REPLACE("a"))


def bad_str_count_too_many_args(root: UnqualifiedNode) -> UnqualifiedNode:
    # Too many arguments to str_count
    return root.nations.CALCULATE(
        str_count1=root.STRCOUNT(root.name, "a", "b"),
    )


def bad_str_count_few_args(root: UnqualifiedNode) -> UnqualifiedNode:
    # Not enough arguments to str_count
    return root.nations.CALCULATE(str_count2=root.STRCOUNT(root.name))


@pytest.mark.parametrize(
    "impl, error_msg",
    [
        pytest.param(
            bad_pydough_impl_01,
            "Unrecognized term of TPCH.nations: 'account_balance'. Did you mean: comment, customers, name, region_key, suppliers, region?",
            id="01",
        ),
        pytest.param(
            bad_pydough_impl_02,
            "PyDough nodes FIZZBUZZ is not callable. Did you mean to use a function?",
            id="02",
        ),
        pytest.param(
            bad_pydough_impl_03,
            "Expected all terms in CALCULATE(y=suppliers.CALCULATE(x=COUNT(supply_records)).x) to be singular, but encountered a plural expression: suppliers.CALCULATE(x=COUNT(supply_records)).x",
            id="03",
        ),
        pytest.param(
            bad_pydough_impl_04,
            "Expected a collection, but received an expression: TPCH.nations.name",
            id="04",
        ),
        pytest.param(
            bad_pydough_impl_05,
            "Expected an expression, but received a collection: nation.region",
            id="05",
        ),
        pytest.param(
            bad_pydough_impl_06,
            "Expected all terms in CALCULATE(o=lines.order.order_date) to be singular, but encountered a plural expression: lines.order.order_date",
            id="06",
        ),
        pytest.param(
            bad_pydough_impl_07,
            "PyDough nodes SUB is not callable. Did you mean to use a function?",
            id="07",
        ),
        pytest.param(
            bad_pydough_impl_08,
            "PyDough objects do not yet support writing properties to them.",
            id="08",
        ),
        pytest.param(
            bad_pydough_impl_09,
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=1, allow_ties=False) == 1).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=1, allow_ties=False) == 1).name",
            id="09",
        ),
        pytest.param(
            bad_pydough_impl_10,
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name",
            id="10",
        ),
        pytest.param(
            bad_pydough_impl_11,
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name",
            id="11",
        ),
        pytest.param(
            bad_pydough_impl_12,
            "Cannot allow ties when multiple best values are requested",
            id="12",
        ),
        pytest.param(
            bad_pydough_impl_13,
            "Per string refers to unrecognized ancestor 'custs' of TPCH.customers.orders",
            id="13",
        ),
        pytest.param(
            bad_pydough_impl_14,
            "Per string 'customers:2' invalid as there are not 2 ancestors of the current context with name 'customers'.",
            id="14",
        ),
        pytest.param(
            bad_pydough_impl_15,
            "Per string 'orders' is ambiguous for TPCH.customers.orders.customer.orders.lines. Use the form 'orders:index' to disambiguate, where 'orders:1' refers to the most recent ancestor.",
            id="15",
        ),
        pytest.param(
            bad_pydough_impl_16,
            "Malformed per string: 'customers:k' (expected the index after ':' to be a positive integer)",
            id="16",
        ),
        pytest.param(
            bad_pydough_impl_17,
            "Malformed per string: 'customers:1:2' (expected 0 or 1 ':', found 2)",
            id="17",
        ),
        pytest.param(
            bad_pydough_impl_18,
            "Malformed per string: 'customers:' (expected the index after ':' to be a positive integer)",
            id="18",
        ),
        pytest.param(
            bad_pydough_impl_19,
            "Malformed per string: 'customers:0' (expected the index after ':' to be a positive integer)",
            id="19",
        ),
        pytest.param(
            bad_pydough_impl_20,
            "PyDough nodes SAMPLE_VAR is not callable. Did you mean to use a function?",
            id="20",
        ),
        pytest.param(
            bad_pydough_impl_21,
            "PyDough nodes SAMPLE_VARIANCE is not callable. Did you mean to use a function?",
            id="21",
        ),
        pytest.param(
            bad_pydough_impl_22,
            "PyDough nodes SAMPLE_STD is not callable. Did you mean to use a function?",
            id="22",
        ),
        pytest.param(
            bad_pydough_impl_23,
            "PyDough nodes POPULATION_STD is not callable. Did you mean to use a function?",
            id="23",
        ),
        pytest.param(
            bad_replace_too_many_args,
            "Expected between 2 and 3 arguments inclusive, received 4",
            id="bad_replace_too_many_args",
        ),
        pytest.param(
            bad_replace_few_args,
            "Expected between 2 and 3 arguments inclusive, received 1",
            id="bad_replace_few_args",
        ),
        pytest.param(
            bad_str_count_too_many_args,
            "Expected 2 arguments, received 3",
            id="bad_str_count_too_many_args",
        ),
        pytest.param(
            bad_str_count_few_args,
            "Expected 2 arguments, received 1",
            id="bad_str_count_few_args",
        ),
    ],
)
def test_qualify_error(
    impl: Callable[[UnqualifiedNode], UnqualifiedNode],
    error_msg: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that strings representing the setup of PyDough unqualified objects
    (with unknown variables already pre-pended with `_ROOT.`) are correctly
    transformed into UnqualifiedNode objects with an expected string
    representation. Each `pydough_str` should be called with `exec` to define
    a variable `answer` that is an `UnqualifiedNode` instance.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = UnqualifiedRoot(graph)
    default_config: PyDoughConfigs = pydough.active_session.config
    with pytest.raises(Exception, match=re.escape(error_msg)):
        unqualified: UnqualifiedNode = impl(root)
        qualify_node(unqualified, graph, default_config)
