"""
Shared helpers for explain and explain_llm. Extracts term lists, conditions,
source collection, and safe qualification so both modules stay consistent.
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
    Returns (expression_names, collection_names) for the node's terms, both
    sorted.

    Iterates node.all_terms; for each name, if get_term(name) is an
    expression, add to expression list, else to collection list.
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
    return (expr_names, collection_names)


def extract_conditions(
    condition: PyDoughExpressionQDAG,
) -> list[PyDoughExpressionQDAG]:
    """
    Returns a list of one or more expression nodes. If condition is an
    ExpressionFunctionCall with operator BAN (AND), returns condition.args;
    otherwise returns [condition].
    """
    if (
        isinstance(condition, ExpressionFunctionCall)
        and condition.operator == pydop.BAN
    ):
        return [arg for arg in condition.args if isinstance(arg, PyDoughExpressionQDAG)]
    return [condition]


# Reserved for use in explain_llm.py (not yet implemented).
def find_source_collection(node: PyDoughCollectionQDAG) -> str | None:
    """
    Returns the name of the first TableCollection found when walking up from
    node via preceding_context. Returns None if none (e.g. global calc or
    user collection).
    """
    current: PyDoughCollectionQDAG | None = node
    while current is not None:
        if isinstance(current, TableCollection):
            return current.collection.name
        current = current.preceding_context
    return None


def qualify_safely(
    node: UnqualifiedNode, session: PyDoughSession
) -> tuple[PyDoughQDAG | None, Exception | None]:
    """
    Qualifies the node with the session. Returns (qualified_node, None) on
    success, or (None, error) on failure. Non-Exception base exceptions
    (KeyboardInterrupt, SystemExit) propagate normally. Callers interpret and
    format messages for caught errors.
    """
    try:
        result = qualify_node(node, session)
        return (result, None)
    except Exception as e:
        return (None, e)
