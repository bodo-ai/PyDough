"""
Definitions of functions used in unit tests in `test_exploration.py`.
"""

__all__ = [
    "contextless_aggfunc_impl",
    "contextless_collections_impl",
    "contextless_expr_impl",
    "contextless_func_impl",
    "customers_without_orders_impl",
    "filter_impl",
    "global_agg_calc_impl",
    "global_calc_impl",
    "global_impl",
    "lineitems_arithmetic_impl",
    "nation_expr_impl",
    "nation_impl",
    "nation_name_impl",
    "nations_lowercase_name_impl",
    "order_by_impl",
    "partition_child_impl",
    "partition_impl",
    "parts_avg_price_child_impl",
    "parts_avg_price_impl",
    "parts_with_german_supplier",
    "region_n_suppliers_in_red_impl",
    "region_nations_back_name",
    "region_nations_suppliers_impl",
    "region_nations_suppliers_name_impl",
    "subcollection_calc_backref_impl",
    "suppliers_iff_balance_impl",
    "table_calc_impl",
    "top_k_impl",
]

from collections.abc import Callable

import pydough
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode


# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def nation_impl() -> UnqualifiedNode:
    return nations


def global_impl() -> UnqualifiedNode:
    return TPCH


def global_calc_impl() -> UnqualifiedNode:
    return TPCH.CALCULATE(x=42, y=13)


def global_agg_calc_impl() -> UnqualifiedNode:
    return TPCH.CALCULATE(
        n_customers=COUNT(customers), avg_part_price=AVG(parts.retail_price)
    )


def table_calc_impl() -> UnqualifiedNode:
    return nations.CALCULATE(
        name, region_name=region.name, num_customers=COUNT(customers)
    )


def subcollection_calc_backref_impl() -> UnqualifiedNode:
    return (
        regions.CALCULATE(region_name=name)
        .nations.CALCULATE(nation_name=name)
        .customers.CALCULATE(name, nation_name, region_name)
    )


def calc_subcollection_impl() -> UnqualifiedNode:
    return nations.CALCULATE(nation_name=name).region


def filter_impl() -> UnqualifiedNode:
    return nations.CALCULATE(nation_name=name).WHERE(
        (region.name == "ASIA")
        & HAS(customers.orders.lines.WHERE(CONTAINS(part.name, "STEEL")))
        & (COUNT(suppliers.WHERE(account_balance >= 0.0)) > 100)
    )


def order_by_impl() -> UnqualifiedNode:
    return nations.CALCULATE(name).ORDER_BY(COUNT(suppliers).DESC(), name.ASC())


def top_k_impl() -> UnqualifiedNode:
    return parts.CALCULATE(name, n_suppliers=COUNT(supply_records)).TOP_K(
        100, by=(n_suppliers.DESC(), name.ASC())
    )


def partition_impl() -> UnqualifiedNode:
    return parts.PARTITION(name="part_types", by=part_type)


def partition_child_impl() -> UnqualifiedNode:
    return (
        parts.PARTITION(name="part_types", by=part_type)
        .CALCULATE(avg_price=AVG(parts.retail_price))
        .WHERE(avg_price >= 27.5)
        .parts
    )


def nation_expr_impl() -> UnqualifiedNode:
    return nations.name


def contextless_expr_impl() -> UnqualifiedNode:
    return name


def contextless_collections_impl() -> UnqualifiedNode:
    return line_items.CALCULATE(extended_price, name=part.name)


def contextless_func_impl() -> UnqualifiedNode:
    return LOWER(first_name + " " + last_name)


def contextless_aggfunc_impl() -> UnqualifiedNode:
    return COUNT(customers)


def nation_name_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return nations, name


def nation_region_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return nations, region


def nation_region_name_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return nations, region.name


def region_nations_suppliers_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return regions, nations.suppliers


def region_nations_suppliers_name_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return regions, nations.suppliers.name


def region_nations_back_name() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return regions.CALCULATE(region_name=name).nations, region_name


def region_n_suppliers_in_red_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return regions, COUNT(nations.suppliers.WHERE(account_balance > 0))


def parts_avg_price_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return parts.PARTITION(name="part_types", by=part_type), AVG(parts.retail_price)


def parts_avg_price_child_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return parts.PARTITION(name="part_types", by=part_type).WHERE(
        AVG(parts.retail_price) >= 27.5
    ), parts


def nations_lowercase_name_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return nations, LOWER(name)


def suppliers_iff_balance_impl() -> UnqualifiedNode:
    return suppliers, IFF(account_balance < 0, 0, account_balance)


def lineitems_arithmetic_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return lines, extended_price * (1 - discount)


def customers_without_orders_impl() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return customers, HASNOT(orders)


def parts_with_german_supplier() -> tuple[UnqualifiedNode, UnqualifiedNode]:
    return parts, HAS(supply_records.supplier.WHERE(nation.name == "GERMANY"))
