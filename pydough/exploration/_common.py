"""
Shared helpers used by both `explain` and `explain_llm`.
"""

__all__ = [
    "describe_expression",
    "describe_subcollection_arg",
    "extract_conditions",
    "extract_terms",
    "find_source_collection",
    "generate_query_summary",
    "generate_step_notes",
    "qualify_safely",
]

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughSession
from pydough.pydough_operators import (
    BinaryOperator,
    SqlAliasExpressionFunctionOperator,
    SqlMacroExpressionFunctionOperator,
    SqlWindowAliasExpressionFunctionOperator,
)
from pydough.qdag import (
    BackReferenceExpression,
    Calculate,
    ChildOperatorChildAccess,
    ChildReferenceCollection,
    ChildReferenceExpression,
    ColumnProperty,
    ExpressionFunctionCall,
    GlobalContext,
    Literal,
    PartitionBy,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    PyDoughQDAG,
    Reference,
    Singular,
    SubCollection,
    TableCollection,
    Where,
    WindowCall,
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


def _is_cross(node: TableCollection) -> bool:
    """
    Returns True if this ``TableCollection`` was produced by a CROSS join.

    When ``left.CROSS(right)`` is qualified, PyDough creates an intermediate
    ``GlobalContext(ancestor=left_collection)`` and uses it as the
    ``ancestor_context`` of the right ``TableCollection``.  A plain root
    table access (e.g. ``nations``) also has a ``GlobalContext`` as its
    ``ancestor_context``, but that GlobalContext's own ``ancestor_context``
    is ``None``.  The nested ancestor is therefore the distinguishing signal.

    Args:
        `node`: a ``TableCollection`` QDAG node.

    Returns:
        ``True`` when the node was produced by a CROSS join, ``False``
        otherwise.
    """
    return (
        isinstance(node.ancestor_context, GlobalContext)
        and node.ancestor_context.ancestor_context is not None
    )


def describe_subcollection_arg(
    collection: PyDoughCollectionQDAG,
) -> dict:
    """
    Produces a structured description of a collection used as an argument to
    an aggregation or predicate operator (``COUNT``, ``NDISTINCT``, ``HAS``,
    ``HASNOT``).

    The description encodes how the collection is accessed: which subcollection
    relationships were traversed (``access_path``) and which explicit filters
    were applied (``filters``).  When the access path is non-empty the
    aggregation is implicitly scoped to each parent row via relationship
    navigation — a correct PyDough pattern — so ``implicit_scope_note`` is set
    to a human-readable explanation.  When the access path is empty (e.g. an
    unrelated collection was cross-joined in) the note is ``None`` and
    ``generate_step_notes`` may emit a scoping warning instead.

    Args:
        `collection`: the QDAG collection node that is the aggregation arg.

    Returns:
        A dict with keys:
        - ``"name"``  — display name for the collection (first subcollection
          property name, or source table name as fallback).
        - ``"access_path"``  — ordered list of subcollection property names
          traversed from the parent context to reach this collection.
        - ``"filters"``  — list of condition strings (``to_string()`` of each
          AND leaf) from any ``Where`` nodes in the chain.
        - ``"implicit_scope_note"``  — explanatory string when ``access_path``
          is non-empty; ``None`` otherwise.
    """
    access_path: list[str] = []
    filters: list[str] = []

    # Walk the preceding_context chain collecting SubCollection hops and
    # Where conditions.  SubCollection.preceding_context is always None
    # (it is a ChildAccess), so the loop naturally terminates there.
    # ChildOperatorChildAccess is a transparent wrapper — unwrap it via
    # .child_access so the walk continues into the real subcollection node.
    current: PyDoughCollectionQDAG | None = collection
    while current is not None:
        if isinstance(current, Where):
            for cond in extract_conditions(current.condition):
                filters.append(cond.to_string())
            current = current.preceding_context
        elif isinstance(current, SubCollection):
            access_path.insert(0, current.subcollection_property.name)
            current = None  # ChildAccess — always None
        elif isinstance(current, ChildOperatorChildAccess):
            current = current.child_access
        else:
            current = getattr(current, "preceding_context", None)

    # Use the first hop as the display name; fall back to the source table.
    name: str = (
        access_path[0]
        if access_path
        else (find_source_collection(collection) or "unknown")
    )

    # Non-empty access_path → row-level scoping via relationship navigation.
    implicit_scope_note: str | None = None
    if access_path:
        path_str = " → ".join(f"'{p}'" for p in access_path)
        implicit_scope_note = (
            f"Aggregating '{name}' which is accessed via relationship "
            f"navigation ({path_str}). Row-level scoping to the parent row "
            f"is implicit in this access path."
        )

    return {
        "name": name,
        "access_path": access_path,
        "filters": filters,
        "implicit_scope_note": implicit_scope_note,
    }


def _resolve_collection_arg(
    arg: PyDoughCollectionQDAG,
    parent: PyDoughCollectionQDAG | None,
) -> PyDoughCollectionQDAG:
    """
    Resolves a collection argument to the underlying collection node.

    Inside a ``CALCULATE``, aggregation arguments are represented as
    ``ChildReferenceCollection`` nodes that point to the parent's child list
    by index.  This function follows that indirection so
    ``describe_subcollection_arg`` receives the raw ``SubCollection`` (or
    user-generated collection) rather than a reference wrapper.

    Args:
        `arg`: the collection arg from an ``ExpressionFunctionCall``.
        `parent`: the parent ``Calculate`` (or other child operator) that owns
          the children list.  May be ``None`` if not available.

    Returns:
        The resolved ``PyDoughCollectionQDAG``, or ``arg`` unchanged if the
        resolution path is unavailable.
    """
    if (
        isinstance(arg, ChildReferenceCollection)
        and parent is not None
        and hasattr(parent, "children")
        and arg.child_idx < len(parent.children)
    ):
        child = parent.children[arg.child_idx]
        if isinstance(child, ChildOperatorChildAccess):
            return child.child_access
    # ChildReferenceCollection always carries the underlying collection via
    # .collection — use it as a fallback when the children-list path fails.
    if isinstance(arg, ChildReferenceCollection):
        return arg.collection
    return arg


def describe_expression(
    expr: PyDoughExpressionQDAG,
    parent: PyDoughCollectionQDAG | None = None,
) -> dict:
    """
    Produces a structured description of a QDAG expression node.

    The ``"kind"`` field identifies the expression category; ``"text"`` is
    always the canonical ``to_string()`` representation.  Aggregation and
    predicate operators whose sole argument is a collection receive the full
    ``describe_subcollection_arg`` treatment so downstream callers (notably
    ``generate_step_notes``) can detect implicit scoping.

    Args:
        `expr`: the QDAG expression node to describe.
        `parent`: the parent collection node (e.g. ``Calculate``) that owns
          the child list.  Needed to resolve ``ChildReferenceCollection``
          arguments; pass ``None`` when not available.

    Returns:
        A dict with at least ``{"kind": str, "text": str}``.  Additional
        keys depend on the expression kind.
    """
    text: str = expr.to_string()

    match expr:
        case Reference():
            return {"kind": "Reference", "term_name": expr.term_name}

        case BackReferenceExpression():
            return {
                "kind": "BackReference",
                "text": text,
                "term_name": expr.term_name,
                "back_levels": expr.back_levels,
            }

        case ChildReferenceExpression():
            return {
                "kind": "ChildReference",
                "text": text,
                "term_name": expr.term_name,
                "child_idx": expr.child_idx,
            }

        case ColumnProperty():
            return {
                "kind": "Column",
                "text": text,
                "collection": expr.column_property.collection.name,
                "column": expr.column_property.name,
                "data_type": expr.pydough_type.json_string,
            }

        case Literal():
            return {
                "kind": "Literal",
                "text": text,
                "value": expr.value,
                "data_type": expr.pydough_type.json_string,
            }

        case ExpressionFunctionCall():
            op = expr.operator
            # Aggregation/predicate operators whose single arg is a collection
            # get the full subcollection description.
            if (
                op in (pydop.COUNT, pydop.NDISTINCT)
                and len(expr.args) == 1
                and isinstance(expr.args[0], PyDoughCollectionQDAG)
            ):
                resolved = _resolve_collection_arg(expr.args[0], parent)
                return {
                    "kind": "Aggregation",
                    "text": text,
                    "function": op.function_name,
                    "args": [describe_subcollection_arg(resolved)],
                }

            if (
                op in (pydop.HAS, pydop.HASNOT)
                and len(expr.args) == 1
                and isinstance(expr.args[0], PyDoughCollectionQDAG)
            ):
                resolved = _resolve_collection_arg(expr.args[0], parent)
                return {
                    "kind": "Predicate",
                    "text": text,
                    "function": op.function_name,
                    "args": [describe_subcollection_arg(resolved)],
                }

            if isinstance(op, BinaryOperator):
                left_arg, right_arg = expr.args[0], expr.args[1]
                return {
                    "kind": "BinaryOp",
                    "text": text,
                    "operator": op.function_name,
                    "left": describe_expression(left_arg, parent)
                    if isinstance(left_arg, PyDoughExpressionQDAG)
                    else {"kind": "Unknown", "text": str(left_arg)},
                    "right": describe_expression(right_arg, parent)
                    if isinstance(right_arg, PyDoughExpressionQDAG)
                    else {"kind": "Unknown", "text": str(right_arg)},
                }

            if isinstance(op, SqlAliasExpressionFunctionOperator):
                return {
                    "kind": "UDFAlias",
                    "text": text,
                    "function": op.function_name,
                    "sql_alias": op.sql_function_alias,
                    "is_aggregation": op.is_aggregation,
                }

            if isinstance(op, SqlMacroExpressionFunctionOperator):
                return {
                    "kind": "UDFMacro",
                    "text": text,
                    "function": op.function_name,
                    "macro_text": op.macro_text,
                    "is_aggregation": op.is_aggregation,
                }

            # Generic built-in function call
            return {
                "kind": "FunctionCall",
                "text": text,
                "function": op.function_name,
                "is_aggregation": op.is_aggregation,
            }

        case WindowCall():
            op = expr.window_operator
            if isinstance(op, SqlWindowAliasExpressionFunctionOperator):
                return {
                    "kind": "UDFWindowCall",
                    "text": text,
                    "function": op.function_name,
                    "sql_alias": op.sql_function_alias,
                }
            return {
                "kind": "WindowCall",
                "text": text,
                "function": op.function_name,
            }

        case _:
            # Fallback — always returns something meaningful
            return {"kind": "Unknown", "text": text}


def generate_step_notes(
    node: PyDoughCollectionQDAG,
    step: dict,
    context_introducing_terms: list[str],
) -> list[str]:
    """
    Produces a list of human-readable notes for a single step in the
    ``explain_llm`` output.

    Notes serve two purposes:
    1. **Informational** — flag correct but non-obvious patterns (e.g.
       implicit row-level scoping via relationship navigation).
    2. **Warning** — flag potentially incorrect patterns (e.g. aggregating a
       cross-joined collection without an explicit filter that ties it to the
       context-introducing terms).

    The ``context_introducing_terms`` list carries expression names that were
    made available by the most recent CROSS or ``PartitionBy`` step.  These
    are the terms that should appear in filters for the aggregation to be
    properly scoped.

    Args:
        `node`: the QDAG node for this step.
        `step`: the already-built step dict (may inspect ``"term_details"``,
          ``"keys"``, ``"child_name"``, etc.).
        `context_introducing_terms`: expression names introduced by the most
          recent context-introducing step (CROSS or PartitionBy).  Reset by
          the step-building loop whenever a new such step is encountered.

    Returns:
        A list of note strings, possibly empty.  Always emitted so the output
        shape is consistent.
    """
    notes: list[str] = []

    match node:
        case Singular():
            notes.append(
                "SINGULAR asserts that this collection is 1-to-1 with its "
                "parent context. The PyDough compiler trusts this declaration "
                "without runtime verification."
            )

        case TableCollection() if _is_cross(node):
            # Both collection names are recoverable from the QDAG structure:
            # the right collection is this node; the left is the ancestor of
            # the intermediate GlobalContext that CROSS qualification inserts.
            assert node.ancestor_context is not None
            assert node.ancestor_context.ancestor_context is not None
            left_name = node.ancestor_context.ancestor_context.name
            right_name = node.collection.name
            notes.append(
                f"Each row now represents a unique combination of "
                f"'{left_name}' \u00d7 '{right_name}' \u2014 both their "
                f"terms are available downstream."
            )

        case PartitionBy():
            keys = step.get("keys", [])
            child_name = step.get("child_name", "")
            notes.append(
                f"The partition key(s) {keys} are available inside child "
                f"scope '{child_name}' but not outside it."
            )

        case Calculate():
            for term_name, detail in step.get("term_details", {}).items():
                if detail.get("kind") != "Aggregation":
                    continue
                for arg in detail.get("args", []):
                    implicit_note = arg.get("implicit_scope_note")
                    if implicit_note is not None:
                        # Correct pattern: scoping is via relationship nav.
                        access_path = arg.get("access_path", [])
                        nav_root = access_path[0] if access_path else arg["name"]
                        notes.append(
                            f"Note: '{term_name}' aggregates '{arg['name']}'"
                            f" \u2014 scoping is implicit via '{nav_root}' "
                            f"relationship navigation, not an explicit filter."
                        )
                    elif context_introducing_terms:
                        # Warn only when context-introducing terms exist but
                        # none appear in this arg's filters.
                        filters_text = " ".join(arg.get("filters", []))
                        missing = [
                            t
                            for t in context_introducing_terms
                            if t not in filters_text
                        ]
                        if missing:
                            notes.append(
                                f"Warning: '{term_name}' aggregates "
                                f"'{arg['name']}' without filtering on "
                                f"context-introducing term(s) {missing}. "
                                f"This may produce unintended cross-product "
                                f"results."
                            )

    return notes


def generate_query_summary(steps: list[dict]) -> str:
    """
    Returns a single deterministic plain-English sentence summarising what a
    PyDough query does.

    Generated purely from the already-built ``steps`` dict —
    no QDAG re-walking, no external calls.  Each clause is omitted when the
    corresponding step type is absent.

    Clause order:
    1. Subject  — ``TableCollection`` / ``Cross`` / ``UserGeneratedCollection``
    2. Filter   — all ``Where`` step conditions joined with ``" and "``
    3. Partition — ``PartitionBy`` keys
    4. Compute  — final ``Calculate`` step (refs + aggregations)
    5. Limit/Order — ``TopK`` or ``OrderBy``

    Args:
        ``steps``: ordered list of step dicts produced by ``_collect_steps``.

    Returns:
        A single sentence ending with ``.``.
    """
    parts: list[str] = []

    # ------------------------------------------------------------------ #
    # 1. Subject                                                           #
    # ------------------------------------------------------------------ #
    cross_step = next((s for s in steps if s["type"] == "Cross"), None)
    table_step = next((s for s in steps if s["type"] == "TableCollection"), None)
    user_step = next((s for s in steps if s["type"] == "UserGeneratedCollection"), None)

    if cross_step:
        parts.append(
            f"Pairs every '{cross_step['left']}' row with every "
            f"'{cross_step['right']}' row"
        )
    elif table_step:
        parts.append(f"Accesses '{table_step['collection']}'")
    elif user_step:
        parts.append(f"Accesses user-generated collection '{user_step['name']}'")
    else:
        # Global-level CALCULATE: infer subject from aggregation subcollection
        # args so the summary names what is actually being counted/aggregated.
        calc_for_subject = next(
            (s for s in reversed(steps) if s["type"] == "Calculate"), None
        )
        agg_colls: list[str] = []
        if calc_for_subject:
            for tname in calc_for_subject.get("terms", []):
                detail = calc_for_subject.get("term_details", {}).get(tname, {})
                if detail.get("kind") == "Aggregation":
                    for arg_d in detail.get("args", []):
                        cname = arg_d.get("name")
                        if cname and cname != "unknown" and cname not in agg_colls:
                            agg_colls.append(cname)
        if agg_colls:
            parts.append(
                "Graph-level aggregation over subcollection(s): "
                + ", ".join(f"'{c}'" for c in agg_colls)
            )
        else:
            return "Graph-level context with no collection access."

    # ------------------------------------------------------------------ #
    # 2. Filter                                                            #
    # ------------------------------------------------------------------ #
    all_conditions: list[str] = []
    for s in steps:
        if s["type"] != "Where":
            continue
        for cond in s.get("conditions", []):
            # conditions are now dicts; use "text" when available, else
            # fall back to condition_summary of the step
            if isinstance(cond, dict):
                all_conditions.append(cond.get("text", str(cond)))
            else:
                all_conditions.append(str(cond))
    if all_conditions:
        parts.append("filtered to rows where " + " and ".join(all_conditions))

    # ------------------------------------------------------------------ #
    # 3. Partition                                                         #
    # ------------------------------------------------------------------ #
    partition_step = next((s for s in steps if s["type"] == "PartitionBy"), None)
    if partition_step:
        keys_str = ", ".join(partition_step.get("keys", []))
        parts.append(f"partitioned by {keys_str}")

    # ------------------------------------------------------------------ #
    # 4. Compute                                                           #
    # ------------------------------------------------------------------ #
    calc_step = next((s for s in reversed(steps) if s["type"] == "Calculate"), None)
    if calc_step:
        ref_terms: list[str] = []
        agg_terms: list[str] = []
        for name in calc_step.get("terms", []):
            detail = calc_step.get("term_details", {}).get(name, {})
            if detail.get("kind") == "Aggregation":
                fn = detail.get("function", "AGG").lower()
                arg_name = (detail.get("args") or [{}])[0].get("name", "?")
                agg_terms.append(f"{fn}({arg_name}) as '{name}'")
            else:
                ref_terms.append(name)

        compute_parts: list[str] = []
        if ref_terms:
            compute_parts.append("selecting " + ", ".join(ref_terms))
        if agg_terms:
            compute_parts.append("computing " + ", ".join(agg_terms))
        if compute_parts:
            parts.append(" and ".join(compute_parts))

    # ------------------------------------------------------------------ #
    # 5. Limit / Order                                                     #
    # ------------------------------------------------------------------ #
    topk_step = next((s for s in steps if s["type"] == "TopK"), None)
    order_step = next((s for s in steps if s["type"] == "OrderBy"), None)

    if topk_step:
        collation = topk_step.get("collation", [])
        if collation:
            by_str = ", ".join(
                f"{c['text']} {c['direction'].lower()}" for c in collation
            )
            parts.append(f"keeping the top {topk_step['limit']} rows by {by_str}")
        else:
            parts.append(f"keeping the top {topk_step['limit']} rows")
    elif order_step:
        collation = order_step.get("collation", [])
        if collation:
            by_str = ", ".join(
                f"{c['text']} {c['direction'].lower()}" for c in collation
            )
            parts.append(f"ordered by {by_str}")

    # ------------------------------------------------------------------ #
    # Compose                                                              #
    # ------------------------------------------------------------------ #
    if not parts:
        return "No operations detected."

    summary = parts[0]
    for p in parts[1:]:
        summary += ", " + p
    return summary.rstrip(".") + "."
