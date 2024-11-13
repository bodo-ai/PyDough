"""
TODO: add file-level docstring.
"""

import datetime
from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import PyDoughCollectionAST
from pydough.unqualified import (
    BACK,
    LOWER,
    SUM,
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)


def pydough_impl_01(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following PyDough snippet:
    ```
    TPCH.Nations(nation_name=name, total_balance=SUM(customers.acctbal))
    ```
    """
    return root.Nations(
        nation_name=root.name, total_balance=SUM(root.customers.acctbal)
    )


def pydough_impl_02(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following PyDough snippet:
    ```
    lines_1994 = orders.WHERE(
        (datetime.date(1994, 1, 1) <= order_date) &
        (order_date < datetime.date(1995, 1, 1))
    ).lines
    lines_1995 = orders.WHERE(
        (datetime.date(1995, 1, 1) <= order_date) &
        (order_date < datetime.date(1996, 1, 1))
    ).lines
    TPCH.Nations.customers(
        name=LOWER(name),
        nation_name=BACK(1).name,
        total_1994=SUM(lines_1994.extended_price - lines_1994.tax / 2),
        total_1995=SUM(lines_1995.extended_price - lines_1995.tax / 2),
    )
    ```
    """
    lines_1994 = root.orders.WHERE(
        (datetime.date(1994, 1, 1) <= root.order_date)
        & (root.order_date < datetime.date(1995, 1, 1))
    ).lines
    lines_1995 = root.orders.WHERE(
        (datetime.date(1995, 1, 1) <= root.order_date)
        & (root.order_date < datetime.date(1996, 1, 1))
    ).lines
    return root.Nations.customers(
        name=LOWER(root.name),
        nation_name=BACK(1).name,
        total_1994=SUM(lines_1994.extended_price - lines_1994.tax / 2),
        total_1995=SUM(lines_1995.extended_price - lines_1995.tax / 2),
    )


@pytest.mark.parametrize(
    "impl, answer_tree_str",
    [
        pytest.param(
            pydough_impl_01,
            "──┬─ TPCH\n"
            "  ├─── TableCollection[Nations]\n"
            "  └─┬─ Calc[nation_name=name, total_balance=SUM($1.acctbal)]\n"
            "    └─┬─ AccessChild\n"
            "      └─── SubCollection[customers]",
            id="01",
        ),
        pytest.param(
            pydough_impl_02,
            "──┬─ TPCH\n"
            "  └─┬─ TableCollection[Nations]\n"
            "    ├─── SubCollection[customers]\n"
            "    └─┬─ Calc[name=LOWER(name), nation_name=BACK(1).name, total_1994=SUM($1.extended_price - ($1.tax / 2)), total_1995=SUM($2.extended_price - ($2.tax / 2))]\n"
            "      ├─┬─ AccessChild\n"
            "      │ ├─── SubCollection[orders]\n"
            "      │ └─┬─ Where[(order_date >= datetime.date(1994, 1, 1)) & (order_date < datetime.date(1995, 1, 1))]\n"
            "      │   └─── SubCollection[lines]\n"
            "      └─┬─ AccessChild\n"
            "        ├─── SubCollection[orders]\n"
            "        └─┬─ Where[(order_date >= datetime.date(1995, 1, 1)) & (order_date < datetime.date(1996, 1, 1))]\n"
            "          └─── SubCollection[lines]",
            id="02",
        ),
    ],
)
def test_qualify_node_to_ast_string(
    impl: Callable[[UnqualifiedNode], UnqualifiedNode],
    answer_tree_str: str,
    get_sample_graph: graph_fetcher,
):
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified AST version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = impl(root)
    qualified: PyDoughCollectionAST = qualify_node(unqualified, graph)
    assert (
        qualified.to_tree_string() == answer_tree_str
    ), "Mismatch between tree string representation of qualified node and expected AST tree string"
