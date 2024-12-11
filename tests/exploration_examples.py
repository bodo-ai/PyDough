"""
TODO: add file-level docstring
"""

__all__ = [
    "nation_impl",
    "global_impl",
    "global_calc_impl",
    "global_agg_calc_impl",
    "table_calc_impl",
    "subcollection_calc_backref_impl",
    "filter_impl",
    "order_impl",
    "top_k_impl",
    "partition_impl",
    "partition_child_impl",
    "nation_expr_impl",
    "contextless_collections_impl",
    "contextless_expr_impl",
    "contextless_back_impl",
    "contextless_aggfunc_impl",
    "contextless_func_impl",
    "nation_name_impl",
    "region_nations_suppliers_impl",
    "region_nations_suppliers_name_impl",
    "region_nations_back_name",
]

from collections.abc import Callable

import pydough
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode


# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def nation_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Nations

    return impl


def global_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return TPCH

    return impl


def global_calc_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return TPCH(x=42, y=13)

    return impl


def global_agg_calc_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return TPCH(
            n_customers=COUNT(Customers), avg_part_price=AVG(Parts.retail_price)
        )

    return impl


def table_calc_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Nations(name, region_name=region.name, num_customers=COUNT(customers))

    return impl


def subcollection_calc_backref_impl(
    graph: GraphMetadata,
) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Regions.nations.customers(
            name, nation_name=BACK(1).name, region_name=BACK(2).name
        )

    return impl


def filter_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Nations(name).WHERE(
            (region.name == "ASIA")
            & HAS(customers.orders.lines.WHERE(CONTAINS(part.name, "STEEL")))
            & (COUNT(suppliers.WHERE(account_balance >= 0.0)) > 100)
        )

    return impl


def order_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Nations(name).ORDER_BY(COUNT(suppliers).DESC(), name.ASC())

    return impl


def top_k_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Parts(name, n_suppliers=COUNT(suppliers_of_part)).TOP_K(
            100, by=(n_suppliers.DESC(), name.ASC())
        )

    return impl


def partition_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return PARTITION(Parts, name="p", by=part_type)

    return impl


def partition_child_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return (
            PARTITION(Parts, name="p", by=part_type)(
                part_type,
                avg_price=AVG(p.retail_price),
            )
            .WHERE(avg_price >= 27.5)
            .p
        )

    return impl


def nation_expr_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Nations.name

    return impl


def contextless_expr_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return name

    return impl


def contextless_collections_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return lines(extended_price, name=part.name)

    return impl


def contextless_back_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return BACK(1).fizz

    return impl


def contextless_func_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return LOWER(first_name + " " + last_name)

    return impl


def contextless_aggfunc_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    @pydough.init_pydough_context(graph)
    def impl():
        return COUNT(customers)

    return impl


def nation_name_impl(
    graph: GraphMetadata,
) -> Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Nations, name

    return impl


def nation_region_impl(
    graph: GraphMetadata,
) -> Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Nations, region

    return impl


def nation_region_name_impl(
    graph: GraphMetadata,
) -> Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Nations, region.name

    return impl


def region_nations_suppliers_impl(
    graph: GraphMetadata,
) -> Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Regions, nations.suppliers

    return impl


def region_nations_suppliers_name_impl(
    graph: GraphMetadata,
) -> Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Regions, nations.suppliers.name

    return impl


def region_nations_back_name(
    graph: GraphMetadata,
) -> Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]:
    @pydough.init_pydough_context(graph)
    def impl():
        return Regions.nations, BACK(1).name

    return impl
