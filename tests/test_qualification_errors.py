"""
TODO: add file-level docstring.
"""

import re
from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)

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
    TPCH.Nations(nation_name=name, total_balance=SUM(acctbal))
    ```
    The problem: there is no property `acctbal` to be accessed from Nations.
    """
    return root.Nations(nation_name=root.name, total_balance=root.SUM(root.acctbal))


def bad_pydough_impl_02(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations(nation_name=FIZZBUZZ(name))
    ```
    The problem: there is no function named FIZZBUZZ, so this looks like a
    CALC term of a subcollection, which cannot be used as an expression inside
    a CALC.
    """
    return root.Nations(nation_name=root.FIZZBUZZ(root.name))


def bad_pydough_impl_03(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations(y=suppliers(x=COUNT(parts_supplied)).x)
    ```
    The problem: `suppliers(x=COUNT(parts_supplied))` is plural with regards
    to Nations, so accessing its `x` property is still plural, therefore it
    cannot be used as a calc term relative to Nations.
    """
    return root.Nations(y=root.suppliers(x=root.COUNT(root.parts_supplied)).x)


def bad_pydough_impl_04(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations.name
    ```
    The problem: Nations.name is an expression, so it cannot be the final
    answer since that must be a collection.
    """
    return root.Nations.name


def bad_pydough_impl_05(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Customer(r=nation.region)
    ```
    The problem: nation.region is a collection, therefore cannot be used as
    an expression in a CALC term.
    """
    return root.Customers(r=root.nation.region)


def bad_pydough_impl_06(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Suppliers.parts_supplied(o=ps_lines.order.order_date)
    ```
    The problem: ps_lines is plural with regards to parts_supplied, therefore
    ps_lines.order.order_date is also plural and it cannot be used as a calc
    term relative to parts_supplied.
    """
    return root.Suppliers.parts_supplied(o=root.ps_lines.order.order_date)


def bad_pydough_impl_07(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Nations.suppliers.parts_supplied(cust_name=BACK(2).customers.name)
    ```
    The problem: customers is plural with regards to BACK(2), therefore
    BACK(2).customers.name is also plural and it cannot be used as a calc
    term relative to parts_supplied.
    """
    return root.Suppliers.parts_supplied(o=root.ps_lines.order.order_date)


def bad_pydough_impl_08(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following invalid PyDough snippet:
    ```
    TPCH.Lineitems(v=MUL(extended_price, SUB(1, discount)))
    ```
    The problem: there is no function named MUL or SUB, so this looks like a
    CALC term of a subcollection, which cannot be used as an expression inside
    a CALC.
    """
    return root.Lineitems(v=root.MUL(root.extended_price, root.SUB(1, root.discount)))


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
            "TPCH.FIZZBUZZ(name=TPCH.name)",
            id="02",
        ),
        pytest.param(
            bad_pydough_impl_03,
            "Expected all terms in (y=suppliers(x=COUNT(parts_supplied)).x) to be singular, but encountered a plural expression: suppliers(x=COUNT(parts_supplied)).x",
            id="03",
        ),
        pytest.param(
            bad_pydough_impl_04,
            "Property 'name' of TPCH.Nations is not a collection",
            id="04",
        ),
        pytest.param(
            bad_pydough_impl_05,
            "Cannot qualify UnqualifiedAccess as an expression: TPCH.nation.region",
            id="05",
        ),
        pytest.param(
            bad_pydough_impl_06,
            "Expected all terms in (o=ps_lines.order.order_date) to be singular, but encountered a plural expression: ps_lines.order.order_date",
            id="06",
        ),
        pytest.param(
            bad_pydough_impl_07,
            "Expected all terms in (o=ps_lines.order.order_date) to be singular, but encountered a plural expression: ps_lines.order.order_date",
            id="07",
        ),
        pytest.param(
            bad_pydough_impl_08,
            "Cannot qualify UnqualifiedCalc as an expression: TPCH.MUL(extended_price=TPCH.extended_price, _expr0=TPCH.SUB(_expr0=1:Int64Type(), discount=TPCH.discount))",
            id="08",
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
    unqualified: UnqualifiedNode = impl(root)
    with pytest.raises(Exception, match=re.escape(error_msg)):
        qualify_node(unqualified, graph)
