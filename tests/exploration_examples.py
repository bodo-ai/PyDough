"""
TODO: add file-level docstring
"""

__all__ = [
    "nation_impl",
    "global_impl",
    "global_calc_impl",
    "global_agg_calc_impl",
]

from collections.abc import Callable

import pydough
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode


# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def nation_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    """
    TODO
    """

    @pydough.init_pydough_context(graph)
    def impl():
        return Nations

    return impl


def global_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    """
    TODO
    """

    @pydough.init_pydough_context(graph)
    def impl():
        return TPCH

    return impl


def global_calc_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    """
    TODO
    """

    @pydough.init_pydough_context(graph)
    def impl():
        return TPCH(x=42, y=13)

    return impl


def global_agg_calc_impl(graph: GraphMetadata) -> Callable[[], UnqualifiedNode]:
    """
    TODO
    """

    @pydough.init_pydough_context(graph)
    def impl():
        return TPCH(
            n_customers=COUNT(Customers), avg_part_price=AVG(Parts.retail_price)
        )

    return impl
