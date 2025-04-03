"""
Error-handling unit tests the PyDough qualification process that transforms
unqualified nodes into qualified DAG nodes.
"""

import re
from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)

import pydough
from pydough.configs import PyDoughConfigs
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)


def bad_pydough_impl_01(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations.CALCULATE(nation_name=name, total_balance=SUM(acctbal))
    ```
    The problem: there is no property `acctbal` to be accessed from Nations.
    """
    return root.Nations.CALCULATE(
        nation_name=root.name, total_balance=root.SUM(root.acctbal)
    )


def bad_pydough_impl_02(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations.CALCULATE(nation_name=FIZZBUZZ(name))
    ```
    The problem: there is no function named FIZZBUZZ, so this looks like a
    CALCULATE being done onto a subcollection, which cannot be used as an
    expression inside a CALCULATE.
    """
    return root.Nations.CALCULATE(nation_name=root.FIZZBUZZ(root.name))


def bad_pydough_impl_03(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations.CALCULATE(y=suppliers.CALCULATE(x=COUNT(parts_supplied)).x)
    ```
    The problem: `suppliers.CALCULATE(x=COUNT(parts_supplied))` is plural with regards
    to Nations, so accessing its `x` property is still plural, therefore it
    cannot be used as a term inside a CALCULATE from the context of Nations.
    """
    return root.Nations.CALCULATE(
        y=root.suppliers.CALCULATE(x=root.COUNT(root.parts_supplied)).x
    )


def bad_pydough_impl_04(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations.name.hello
    ```
    The problem: Nations.name is an expression, so invoking `.hello` on it is
    not valid.
    """
    return root.Nations.name.hello


def bad_pydough_impl_05(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Customer(r=nation.region)
    ```
    The problem: nation.region is a collection, therefore cannot be used as
    an expression in a CALCULATE.
    """
    return root.Customers.CALCULATE(r=root.nation.region)


def bad_pydough_impl_06(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Suppliers.supply_records.CALCULATE(o=lines.order.order_date)
    ```
    The problem: lines is plural with regards to supply_records, therefore
    lines.order.order_date is also plural and it cannot be used in a CALCULATE
    in the context of supply_records.
    """
    return root.Suppliers.supply_records.CALCULATE(o=root.lines.order.order_date)


def bad_pydough_impl_07(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Lineitems.CALCULATE(v=MUL(extended_price, SUB(1, discount)))
    ```
    The problem: there is no function named MUL or SUB, so this looks like a
    CALCULATE operation on a subcollection, which cannot be used as an
    expression inside of a CALCULATE.
    """
    return root.Lineitems.CALCULATE(
        v=root.MUL(root.extended_price, root.SUB(1, root.discount))
    )


def bad_pydough_impl_08(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Lineitems.tax = 0
    TPCH.Lineitems.CALCULATE(value=extended_price * tax)
    ```
    The problem: writing to an unqualified node is not yet supported.
    """
    root.Lineitems.tax = 0
    return root.Lineitems.CALCULATE(value=root.extended_price * root.tax)


def bad_pydough_impl_09(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    best_customer = nations.BEST(customer, by=acctbal.DESC())
    Regions.CALCULATE(n=best_customer.name)
    ```
    The problem: The cardinality is off since even though the `BEST` ensures
    the customers are singular with regards to the nation, the nations are
    still plural with regards to the region.
    """
    best_customer = root.nations.BEST(root.customers, by=root.acctbal.DESC())
    return root.Regions.CALCULATE(n=best_customer.name)


def bad_pydough_impl_10(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    best_customer = BEST(nations.customers, by=acctbal.DESC(), allow_ties=True)
    Regions.CALCULATE(n=best_customer.name)
    ```
    The problem: the presence of `allow_ties=True` means that the `BEST`
    operator does not guarantee `nations.customers` is plural with regards to
    `Regions`.
    """
    best_customer = root.BEST(
        root.nations.customers, by=root.acctbal.DESC(), allow_ties=True
    )
    return root.Regions.CALCULATE(n=best_customer.name)


def bad_pydough_impl_11(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    best_customer = BEST(nations.customers, by=acctbal.DESC(), n_best=3)
    Regions.CALCULATE(n=best_customer.name)
    ```
    The problem: the presence of `n_best=3` means that the `BEST` operator
    does not guarantee `nations.customers` is plural with regards to `Regions`.
    """
    best_customer = root.BEST(
        root.nations.customers, by=root.acctbal.DESC(), allow_ties=True
    )
    return root.Regions.CALCULATE(n=best_customer.name)


def bad_pydough_impl_12(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    Regions.BEST(nations.customers, by=acctbal.DESC(), n_best=3, allow_ties=True)
    ```
    The problem: cannot simultaneously use `n_best=3` and `allow_ties=True`.
    """
    return root.Regions.BEST(
        root.nations.customers, by=root.acctbal.DESC(), n_best=3, allow_ties=True
    )


@pytest.mark.parametrize(
    "impl, error_msg",
    [
        pytest.param(
            bad_pydough_impl_01,
            "Unrecognized term of simple table collection 'Nations' in graph 'TPCH': 'acctbal'",
            id="01",
        ),
        pytest.param(
            bad_pydough_impl_02,
            "PyDough nodes FIZZBUZZ is not callable. Did you mean to use a function?",
            id="02",
        ),
        pytest.param(
            bad_pydough_impl_03,
            "Expected all terms in CALCULATE(y=suppliers.CALCULATE(x=COUNT(parts_supplied)).x) to be singular, but encountered a plural expression: suppliers.CALCULATE(x=COUNT(parts_supplied)).x",
            id="03",
        ),
        pytest.param(
            bad_pydough_impl_04,
            "Expected a collection, but received an expression: TPCH.Nations.name",
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
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(acctbal.DESC(na_pos='last')), levels=1, allow_ties=False) == 1).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(acctbal.DESC(na_pos='last')), levels=1, allow_ties=False) == 1).name",
            id="09",
        ),
        pytest.param(
            bad_pydough_impl_10,
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(acctbal.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(acctbal.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name",
            id="10",
        ),
        pytest.param(
            bad_pydough_impl_11,
            "Expected all terms in CALCULATE(n=nations.customers.WHERE(RANKING(by=(acctbal.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name) to be singular, but encountered a plural expression: nations.customers.WHERE(RANKING(by=(acctbal.DESC(na_pos='last')), levels=2, allow_ties=True) == 1).name",
            id="11",
        ),
        pytest.param(
            bad_pydough_impl_12,
            "Cannot allow ties when multiple best values are requested",
            id="12",
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
