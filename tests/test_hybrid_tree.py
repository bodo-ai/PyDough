"""
Unit tests for cardinality-inference properties of `HybridTree`, such as
`is_singular` and `is_reverse_singular`.
"""

from collections.abc import Callable

import pytest

from pydough import init_pydough_context
from pydough.configs import PyDoughSession
from pydough.conversion.hybrid_translator import HybridTranslator
from pydough.conversion.hybrid_tree import HybridTree
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.unqualified import UnqualifiedNode, qualify_node
from tests.test_pydough_functions.simple_pydough_functions import (
    tablescan_plural,
    tablescan_singular,
)
from tests.test_pydough_functions.user_collections import (
    crossed_singleton,
    standalone_singleton,
)


def build_hybrid_tree(
    session: PyDoughSession, impl: Callable[[], UnqualifiedNode]
) -> HybridTree:
    """
    Builds the `HybridTree` for the collection returned by a test impl
    function, without needing a live database connection.
    """
    assert session.metadata is not None
    unqualified: UnqualifiedNode = init_pydough_context(session.metadata)(impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, session)
    assert isinstance(qualified, PyDoughCollectionQDAG)
    return HybridTranslator(session).make_hybrid_tree(
        qualified, parent=None, is_aggregate=False
    )


@pytest.mark.parametrize(
    "impl, expected",
    [
        pytest.param(standalone_singleton, True, id="standalone_singleton"),
        pytest.param(crossed_singleton, False, id="crossed_singleton"),
        pytest.param(tablescan_singular, True, id="tablescan_singular"),
        pytest.param(tablescan_plural, False, id="tablescan_plural"),
    ],
)
def test_is_reverse_singular(
    impl: Callable[[], UnqualifiedNode],
    expected: bool,
    empty_sqlite_tpch_session: PyDoughSession,
) -> None:
    """
    Tests that `HybridTree.is_reverse_singular()` correctly distinguishes
    a collection sitting directly under the root from the same collection
    nested below a `CROSS`, regardless of whether it only has one row.
    """
    tree: HybridTree = build_hybrid_tree(empty_sqlite_tpch_session, impl)
    assert tree.is_reverse_singular() is expected


@pytest.mark.parametrize(
    "impl, expected",
    [
        pytest.param(standalone_singleton, True, id="standalone_singleton"),
        pytest.param(crossed_singleton, False, id="crossed_singleton"),
    ],
)
def test_is_singular(
    impl: Callable[[], UnqualifiedNode],
    expected: bool,
    empty_sqlite_tpch_session: PyDoughSession,
) -> None:
    """
    Tests that `HybridTree.is_singular()` correctly distinguishes a
    collection sitting directly under the root from the same collection
    nested below a `CROSS`.
    """
    tree: HybridTree = build_hybrid_tree(empty_sqlite_tpch_session, impl)
    assert tree.is_singular() is expected
