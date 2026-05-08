"""
Shared helpers used by both `explain` and `explain_llm`.
"""

__all__ = [
    "extract_conditions",
    "extract_terms",
    "find_source_collection",
    "qualify_safely",
]

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughSession
from pydough.qdag import (
    ExpressionFunctionCall,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    PyDoughQDAG,
    TableCollection,
)
from pydough.unqualified import UnqualifiedNode, qualify_node


def extract_terms(
    node: PyDoughCollectionQDAG,
) -> tuple[list[str], list[str]]:
    """
    Splits the terms of a qualified collection into sorted lists of expression
    names and collection names.

    Args:
        `node`: the qualified collection whose terms are being extracted.

    Returns:
        A tuple ``(expression_names, collection_names)``, both sorted
        alphabetically.
    """
    expr_names: list[str] = []
    collection_names: list[str] = []
    for name in node.all_terms:
        term: PyDoughQDAG = node.get_term(name)
        if isinstance(term, PyDoughExpressionQDAG):
            expr_names.append(name)
        else:
            collection_names.append(name)
    expr_names.sort()
    collection_names.sort()
    return expr_names, collection_names


def extract_conditions(
    condition: PyDoughExpressionQDAG,
) -> list[PyDoughExpressionQDAG]:
    """
    Flattens a condition expression into a list of its AND sub-conditions.
    Nested AND (BAN) nodes are recursively split; all other operators are
    returned as a single-element list.

    Args:
        `condition`: the top-level condition expression to flatten.

    Returns:
        A flat list of leaf-level AND sub-conditions.
    """
    if (
        isinstance(condition, ExpressionFunctionCall)
        and condition.operator == pydop.BAN
    ):
        result: list[PyDoughExpressionQDAG] = []
        for arg in condition.args:
            assert isinstance(arg, PyDoughExpressionQDAG)
            result.extend(extract_conditions(arg))
        return result
    return [condition]


def find_source_collection(node: PyDoughCollectionQDAG) -> str | None:
    """
    Walks up the ``preceding_context`` chain of a qualified collection and
    returns the name of the first ``TableCollection`` found.

    Args:
        `node`: the qualified collection to inspect.

    Returns:
        The name of the source table collection, or ``None`` if none is found
        (e.g. global calc or user-generated collection root).
    """
    current: PyDoughCollectionQDAG | None = node
    while current is not None:
        if isinstance(current, TableCollection):
            return current.collection.name
        current = getattr(current, "preceding_context", None)
    return None


def qualify_safely(
    node: UnqualifiedNode,
    session: PyDoughSession,
) -> tuple[PyDoughQDAG | None, BaseException | None]:
    """
    Attempts to qualify an unqualified node, returning the result and any
    error as a tuple rather than raising.

    Args:
        `node`: the unqualified node to qualify.
        `session`: the PyDough session to use for qualification.

    Returns:
        ``(qualified_node, None)`` on success, or ``(None, exception)`` on
        failure. The result is the base ``PyDoughQDAG`` type; callers narrow
        to ``PyDoughCollectionQDAG`` or ``PyDoughExpressionQDAG`` as needed.
    """
    try:
        return qualify_node(node, session), None
    except Exception as e:
        return None, e
