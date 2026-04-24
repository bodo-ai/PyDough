"""
Implementation of ``pydough.explain_llm``, which returns a structured JSON-
serialisable dict describing a PyDough collection expression.

The output is designed for LLM consumption: deterministic shape, stable
``kind`` tags, and explicit scoping notes so a model can self-correct without
parsing prose.

Output schema (success)::

    {
        "error": false,
        "query_summary": "<deterministic plain-English sentence>",
        "steps": [
            {
                "order": 1,
                "type": "<step type>",
                "description": "<short stable phrase>",
                # type-specific fields …
                "debug": {
                    "available_terms": {
                        "expressions": ["name", ...],
                        "collections": ["orders", ...]
                    }
                },
                "notes": ["…"]
            },
            …
        ],
        "schema": {
            "source_collection": "<name or null>",
            "available_expressions": ["…"],
            "output_columns": ["…"],
            "column_types": {"name": "string", …},
            "ordering": [{"text": "name", "direction": "ASC", "nulls": "LAST"}],
            "limit": null
        }
    }

Output schema (error)::

    {
        "error": true,
        "message": "<qualification error text>",
        "steps": [],
        "schema": null
    }

When ``format="md"`` is passed the same information is returned as a markdown
string, structured into ``## Query Summary``, ``## Steps``, and ``## Schema``
sections — easier for an LLM judge to read than raw JSON.
"""

__all__ = ["explain_llm"]

import pydough
import pydough.pydough_operators as pydop
from pydough.configs import PyDoughSession
from pydough.qdag import (
    Calculate,
    ExpressionFunctionCall,
    GlobalContext,
    OrderBy,
    PartitionBy,
    PartitionChild,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    Singular,
    SubCollection,
    TableCollection,
    TopK,
    Where,
)
from pydough.qdag.collections.user_collection_qdag import (
    PyDoughUserGeneratedCollectionQDag,
)
from pydough.unqualified import UnqualifiedNode

from ._common import (
    _is_cross,
    _resolve_collection_arg,
    describe_expression,
    extract_conditions,
    extract_terms,
    find_source_collection,
    generate_query_summary,
    generate_step_notes,
    qualify_safely,
)

# ---------------------------------------------------------------------------
# Error payload helper
# ---------------------------------------------------------------------------


def _error_payload(message: str) -> dict:
    return {
        "error": True,
        "message": message,
        "steps": [],
        "schema": None,
    }


# ---------------------------------------------------------------------------
# Step builders — one per QDAG node type
# ---------------------------------------------------------------------------


def _build_global_context_step(node: GlobalContext, order: int) -> dict:
    """
    The root step every expression starts with.

    GlobalContext represents the top-level graph — one implicit row, access to
    all top-level collections.  There are no calc_terms here; available
    expressions come from inherited terms (if this is a CROSS intermediate
    GlobalContext they'll be populated, but for the true root it's empty).
    """
    expr_names, coll_names = extract_terms(node)
    return {
        "order": order,
        "type": "GlobalContext",
        "description": "Entry point: the graph-level context.",
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }


def _build_table_collection_step(node: TableCollection, order: int) -> dict:
    """
    Accesses a named collection (table) from the graph.

    CROSS joins also produce a TableCollection but with a nested intermediate
    GlobalContext as their ancestor (see ``_is_cross``).  When detected, the
    step type becomes ``"Cross"`` and both collection names are recorded.
    """
    expr_names, coll_names = extract_terms(node)
    debug = {"available_terms": {"expressions": expr_names, "collections": coll_names}}

    if _is_cross(node):
        assert node.ancestor_context is not None
        assert node.ancestor_context.ancestor_context is not None
        left_name = node.ancestor_context.ancestor_context.name
        right_name = node.collection.name
        return {
            "order": order,
            "type": "Cross",
            "description": (
                f"CROSS join: every row of '{left_name}' paired with every "
                f"row of '{right_name}'."
            ),
            "left": left_name,
            "right": right_name,
            "debug": debug,
        }

    collection_name = node.collection.name
    return {
        "order": order,
        "type": "TableCollection",
        "description": f"Accesses the '{collection_name}' collection.",
        "collection": collection_name,
        "debug": debug,
    }


def _build_sub_collection_step(node: SubCollection, order: int) -> dict:
    """
    Traverses a subcollection relationship (implicit JOIN in SQL).

    The property name (e.g. ``"orders"``) and the collections it connects are
    recorded.  The caller can use these to understand relationship navigation.
    """
    prop = node.subcollection_property
    expr_names, coll_names = extract_terms(node)
    return {
        "order": order,
        "type": "SubCollection",
        "description": (
            f"Traverses the '{prop.name}' relationship from "
            f"'{prop.collection.name}' to '{prop.child_collection.name}'."
        ),
        "property": prop.name,
        "from_collection": prop.collection.name,
        "to_collection": prop.child_collection.name,
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }


def _build_where_step(node: Where, order: int) -> dict:
    """
    Filters rows to those satisfying one or more conditions.

    Multi-condition ANDs are split via ``extract_conditions`` so each
    predicate appears as a separate element in ``conditions``.  Each condition
    is described as a structured dict via ``describe_expression`` rather than a
    plain string, so the judge can inspect operator, left, and right operands
    without parsing.  ``condition_summary`` is kept as a plain string for quick
    human reading.
    """
    raw_conditions = extract_conditions(node.condition)
    conditions = [describe_expression(c) for c in raw_conditions]
    summary = (
        raw_conditions[0].to_string()
        if len(raw_conditions) == 1
        else " & ".join(c.to_string() for c in raw_conditions)
    )
    expr_names, coll_names = extract_terms(node)
    return {
        "order": order,
        "type": "Where",
        "description": "Filters rows to those matching the given conditions.",
        "conditions": conditions,
        "condition_summary": summary,
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }


def _build_calculate_step(node: Calculate, order: int) -> dict:
    """
    Adds computed expressions to the collection.

    ``term_details`` maps each new term name to its ``describe_expression``
    dict.  Terms are sorted by their declared position so the order is stable
    and matches the user's CALCULATE(...) call.
    """
    sorted_terms = sorted(
        node.calc_terms,
        key=lambda name: node.get_expression_position(name),
    )
    term_details = {
        name: describe_expression(node.get_expr(name), parent=node)
        for name in sorted_terms
    }
    expr_names, coll_names = extract_terms(node)
    return {
        "order": order,
        "type": "Calculate",
        "description": "Adds computed expressions to the collection.",
        "terms": sorted_terms,
        "term_details": term_details,
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }


def _build_order_by_step(node: OrderBy, order: int) -> dict:
    """
    Sorts the collection by one or more collation keys.

    Each key records direction (``"ASC"``/``"DESC"``) and null placement
    (``"FIRST"``/``"LAST"``).  TopK is a subclass of OrderBy so the same
    builder handles both; ``limit`` is included only for TopK.
    """
    collation = [
        {
            "text": col.expr.to_string(),
            "direction": "ASC" if col.asc else "DESC",
            "nulls": "LAST" if col.na_last else "FIRST",
        }
        for col in node.collation
    ]
    expr_names, coll_names = extract_terms(node)
    step: dict = {
        "order": order,
        "type": "TopK" if isinstance(node, TopK) else "OrderBy",
        "description": (
            f"Sorts the collection and keeps the top {node.records_to_keep} records."
            if isinstance(node, TopK)
            else "Sorts the collection."
        ),
        "collation": collation,
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }
    if isinstance(node, TopK):
        step["limit"] = node.records_to_keep
    return step


def _build_partition_by_step(node: PartitionBy, order: int) -> dict:
    """
    Partitions the collection on one or more key expressions.

    ``keys`` are the partitioning field names; ``child_name`` is the name of
    the subcollection holding the per-partition rows (accessible inside
    aggregations).
    """
    keys = [k.expr.to_string() for k in node.keys]
    child_expr_names, child_coll_names = extract_terms(node.child)
    expr_names, coll_names = extract_terms(node)
    return {
        "order": order,
        "type": "PartitionBy",
        "description": f"Partitions the collection by {keys}.",
        "keys": keys,
        "child_name": node.child.name,
        "child_available_terms": {
            "expressions": child_expr_names,
            "collections": child_coll_names,
        },
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }


def _build_partition_child_step(node: PartitionChild, order: int) -> dict:
    """
    Accesses the unpartitioned child data inside a PARTITION context.
    """
    expr_names, coll_names = extract_terms(node)
    return {
        "order": order,
        "type": "PartitionChild",
        "description": (
            f"Accesses the unpartitioned child data "
            f"(child name: '{node.partition_child_name}')."
        ),
        "child_name": node.partition_child_name,
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }


def _build_singular_step(node: Singular, order: int) -> dict:
    """
    Asserts that the preceding collection is 1-to-1 with its parent context.
    """
    expr_names, coll_names = extract_terms(node)
    return {
        "order": order,
        "type": "Singular",
        "description": (
            "Asserts this collection is singular (1-to-1) with respect to "
            "the parent context."
        ),
        "preceding": node.preceding_context.to_string(),
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }


def _build_user_generated_step(
    node: PyDoughUserGeneratedCollectionQDag, order: int
) -> dict:
    """
    Accesses a user-generated collection (e.g. a ``to_table()`` view).
    """
    expr_names, coll_names = extract_terms(node)
    return {
        "order": order,
        "type": "UserGeneratedCollection",
        "description": f"Accesses user-generated collection '{node.name}'.",
        "name": node.name,
        "debug": {
            "available_terms": {
                "expressions": expr_names,
                "collections": coll_names,
            }
        },
    }


# ---------------------------------------------------------------------------
# Step-walking loop
# ---------------------------------------------------------------------------


def _collect_steps(root: PyDoughCollectionQDAG) -> list[dict]:
    """
    Walks the qualified QDAG chain backward via ``preceding_context``, builds
    a step dict for each node, then reverses the list so step 1 is the
    earliest operation (GlobalContext or TableCollection).

    ``context_introducing_terms`` tracks expression names introduced by the
    most recent CROSS or PartitionBy step.  These are reset each time such a
    step is encountered and passed to ``generate_step_notes`` for the
    aggregation scope check.

    Args:
        `root`: the qualified root QDAG collection node (deepest in the chain).

    Returns:
        An ordered list of step dicts in execution order.
    """
    # Collect nodes from deepest → shallowest
    nodes: list[PyDoughCollectionQDAG] = []
    current: PyDoughCollectionQDAG | None = root
    while current is not None:
        nodes.append(current)
        current = getattr(current, "preceding_context", None)

    # The GlobalContext at the top of the preceding_context chain is missing
    # (TableCollection.preceding_context is None via ChildAccess).  Add it.
    # Walk ancestor_context to reach the true root GlobalContext.
    top = nodes[-1]
    ancestor: PyDoughCollectionQDAG | None = getattr(top, "ancestor_context", None)
    while ancestor is not None and not isinstance(ancestor, GlobalContext):
        ancestor = getattr(ancestor, "ancestor_context", None)
    if ancestor is not None and not any(isinstance(n, GlobalContext) for n in nodes):
        nodes.append(ancestor)

    nodes.reverse()  # now shallowest → deepest (execution order)

    steps: list[dict] = []
    context_introducing_terms: list[str] = []

    for order, node in enumerate(nodes, start=1):
        # Build the type-specific step dict
        match node:
            case GlobalContext():
                step = _build_global_context_step(node, order)
            case TableCollection():
                step = _build_table_collection_step(node, order)
                # CROSS introduces both collections' expression terms
                if _is_cross(node):
                    context_introducing_terms = step["debug"]["available_terms"][
                        "expressions"
                    ]
            case SubCollection():
                step = _build_sub_collection_step(node, order)
            case Where():
                step = _build_where_step(node, order)
            case Calculate():
                step = _build_calculate_step(node, order)
            case PartitionBy():
                step = _build_partition_by_step(node, order)
                # PartitionBy introduces its key expression names as
                # context-introducing terms for child-scope aggregations
                context_introducing_terms = step["keys"]
            case PartitionChild():
                step = _build_partition_child_step(node, order)
            case Singular():
                step = _build_singular_step(node, order)
            case PyDoughUserGeneratedCollectionQDag():
                step = _build_user_generated_step(node, order)
            case OrderBy():
                # TopK is a subclass of OrderBy — handled by same builder
                step = _build_order_by_step(node, order)
            case _:
                # Defensive fallback for any unrecognised node types
                expr_names, coll_names = extract_terms(node)
                step = {
                    "order": order,
                    "type": node.__class__.__name__,
                    "description": "Unrecognised step type.",
                    "debug": {
                        "available_terms": {
                            "expressions": expr_names,
                            "collections": coll_names,
                        }
                    },
                }

        step["notes"] = generate_step_notes(node, step, context_introducing_terms)

        # Reset context_introducing_terms after a non-CROSS, non-Partition step
        # so warnings only fire for the immediately following Calculate.
        if not isinstance(node, (TableCollection, PartitionBy)):
            context_introducing_terms = []

        steps.append(step)

    return steps


# ---------------------------------------------------------------------------
# Schema builder
# ---------------------------------------------------------------------------


def _build_schema(root: PyDoughCollectionQDAG) -> dict:
    """
    Builds the ``schema`` portion of the ``explain_llm`` output from the
    qualified root node.

    ``column_types`` uses ``pydough_type.json_string`` when the expression
    exposes a type; falls back to ``"unknown"`` when the type cannot be
    determined statically (e.g. for references that require context to
    resolve).

    ``ordering`` and ``limit`` are derived by walking the QDAG chain for the
    nearest ``OrderBy`` / ``TopK`` node.  ``available_collections`` is omitted
    — which relationships are reachable at the final step is not relevant to
    correctness judgment.

    Args:
        `root`: the qualified root QDAG collection node.

    Returns:
        The schema dict.
    """
    expr_names, _ = extract_terms(root)
    # Only expose output_columns when there is an explicit CALCULATE at the
    # root.  Without one, calc_terms inherits the predecessor's terms, which
    # would imply a SELECT * — not meaningful as named output columns.
    output_columns = sorted(root.calc_terms) if isinstance(root, Calculate) else []
    column_types: dict[str, str] = {}
    for name in output_columns:
        try:
            expr = root.get_expr(name)
            column_types[name] = expr.pydough_type.json_string
        except Exception:
            column_types[name] = "unknown"

    # Walk the QDAG chain once to find ordering / limit.
    ordering: list[dict] = []
    limit: int | None = None
    current: PyDoughCollectionQDAG | None = root
    while current is not None:
        if isinstance(current, TopK):
            ordering = [
                {
                    "text": col.expr.to_string(),
                    "direction": "ASC" if col.asc else "DESC",
                    "nulls": "LAST" if col.na_last else "FIRST",
                }
                for col in current.collation
            ]
            limit = current.records_to_keep
            break
        if isinstance(current, OrderBy):
            ordering = [
                {
                    "text": col.expr.to_string(),
                    "direction": "ASC" if col.asc else "DESC",
                    "nulls": "LAST" if col.na_last else "FIRST",
                }
                for col in current.collation
            ]
            break
        current = getattr(current, "preceding_context", None)

    # find_source_collection returns None for global-level CALCULATEs; fall
    # back to inspecting aggregation args across the QDAG chain so the schema
    # still names the collection(s) being accessed.
    source = find_source_collection(root)
    if source is None:
        cur: PyDoughCollectionQDAG | None = root
        while cur is not None and source is None:
            if isinstance(cur, Calculate):
                for tname in cur.calc_terms:
                    try:
                        expr = cur.get_expr(tname)
                    except Exception:
                        continue
                    if not isinstance(expr, ExpressionFunctionCall):
                        continue
                    if expr.operator not in (pydop.COUNT, pydop.NDISTINCT):
                        continue
                    if not expr.args or not isinstance(
                        expr.args[0], PyDoughCollectionQDAG
                    ):
                        continue
                    resolved = _resolve_collection_arg(expr.args[0], cur)
                    candidate = find_source_collection(resolved)
                    if candidate and candidate != "unknown":
                        source = candidate
                        break
            cur = getattr(cur, "preceding_context", None)

    return {
        "source_collection": source,
        "available_expressions": expr_names,
        "output_columns": output_columns,
        "column_types": column_types,
        "ordering": ordering,
        "limit": limit,
    }


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------


def _expr_display(detail: dict) -> str:
    """Returns a short display string for a ``describe_expression`` dict.

    ``Reference`` dicts have no ``text`` field (removed in Phase 2), so we
    fall back to ``term_name``.  All other kinds carry a ``text`` field.
    """
    if detail.get("kind") == "Reference":
        return detail.get("term_name", "?")
    return detail.get("text", detail.get("kind", "?"))


def _render_step_body(step: dict) -> list[str]:
    """Returns the type-specific body lines for a single step dict."""
    lines: list[str] = []
    stype = step["type"]

    if stype == "TableCollection":
        lines.append(f"- Collection: `{step['collection']}`")

    elif stype == "Cross":
        lines.append(f"- Left: `{step['left']}`")
        lines.append(f"- Right: `{step['right']}`")

    elif stype == "SubCollection":
        lines.append(
            f"- `{step['from_collection']}` → `{step['to_collection']}`"
            f" via `{step['property']}`"
        )

    elif stype == "Where":
        conditions = step.get("conditions", [])
        if len(conditions) == 1:
            cond = conditions[0]
            ctext = _expr_display(cond) if isinstance(cond, dict) else str(cond)
            lines.append(f"- Condition: `{ctext}`")
        elif conditions:
            lines.append("- Conditions:")
            for cond in conditions:
                ctext = _expr_display(cond) if isinstance(cond, dict) else str(cond)
                lines.append(f"  - `{ctext}`")

    elif stype == "Calculate":
        terms = step.get("terms", [])
        term_details = step.get("term_details", {})
        if terms:
            lines.append("- Terms:")
            for name in terms:
                detail = term_details.get(name, {})
                kind = detail.get("kind", "Unknown")
                if kind == "Reference":
                    ref = detail.get("term_name", name)
                    suffix = f"reference to `{ref}`" if ref != name else "reference"
                    lines.append(f"  - `{name}` → {suffix}")
                elif kind == "Aggregation":
                    fn = detail.get("function", "AGG")
                    args = detail.get("args", [])
                    arg_name = args[0]["name"] if args else "?"
                    implicit = args[0].get("implicit_scope_note") if args else None
                    filters = args[0].get("filters", []) if args else []
                    scope = (
                        " _(implicitly scoped via relationship)_" if implicit else ""
                    )
                    filter_str = " where " + " AND ".join(filters) if filters else ""
                    lines.append(
                        f"  - `{name}` → {fn}(`{arg_name}`){filter_str}{scope}"
                    )
                else:
                    lines.append(f"  - `{name}` → {detail.get('text', kind)}")

    elif stype in ("OrderBy", "TopK"):
        if stype == "TopK":
            lines.append(f"- Limit: {step['limit']}")
        collation = step.get("collation", [])
        if collation:
            col_str = ", ".join(
                f"`{c['text']}` {c['direction']} NULLS {c['nulls']}" for c in collation
            )
            lines.append(f"- Order by: {col_str}")

    elif stype == "PartitionBy":
        keys = step.get("keys", [])
        lines.append("- Keys: " + ", ".join(f"`{k}`" for k in keys))
        lines.append(f"- Child name: `{step['child_name']}`")

    elif stype == "PartitionChild":
        lines.append(f"- Child name: `{step['child_name']}`")

    elif stype == "Singular":
        lines.append(f"- Preceding: `{step.get('preceding', '')}`")

    elif stype == "UserGeneratedCollection":
        lines.append(f"- Name: `{step['name']}`")

    return lines


def _render_md(result: dict) -> str:
    """
    Renders an ``explain_llm`` result dict as a markdown string.

    Produces three top-level sections: **Query Summary**, **Steps**, and
    **Schema**.  Each step is a ``###`` sub-heading with its type-specific
    fields rendered as a bullet list and any notes as block-quotes.

    Error payloads become a single ``## Error`` section.

    Args:
        `result`: a dict produced by the JSON path of ``explain_llm``.

    Returns:
        A markdown string.
    """
    lines: list[str] = []

    if result["error"]:
        lines.append("## Error")
        lines.append("")
        lines.append(result["message"])
        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    # Query Summary                                                        #
    # ------------------------------------------------------------------ #
    lines.append("## Query Summary")
    lines.append("")
    lines.append(result["query_summary"])
    lines.append("")

    # ------------------------------------------------------------------ #
    # Steps                                                                #
    # ------------------------------------------------------------------ #
    lines.append("## Steps")
    lines.append("")
    for step in result["steps"]:
        lines.append(f"### Step {step['order']} — {step['type']}")
        lines.append("")
        lines.append(step["description"])
        lines.append("")

        body = _render_step_body(step)
        lines.extend(body)

        notes = step.get("notes", [])
        if notes:
            if body:
                lines.append("")
            for note in notes:
                lines.append(f"> {note}")

        lines.append("")

    # ------------------------------------------------------------------ #
    # Schema                                                               #
    # ------------------------------------------------------------------ #
    schema = result["schema"]
    lines.append("## Schema")
    lines.append("")

    src = schema.get("source_collection")
    lines.append(f"- **Source collection:** {f'`{src}`' if src else '_(none)_'}")

    output_cols = schema.get("output_columns", [])
    col_types = schema.get("column_types", {})
    if output_cols:
        col_parts = ", ".join(
            f"`{c}` ({col_types.get(c, 'unknown')})" for c in output_cols
        )
        lines.append(f"- **Output columns:** {col_parts}")
    else:
        lines.append("- **Output columns:** _(none)_")

    ordering = schema.get("ordering", [])
    if ordering:
        ord_str = ", ".join(
            f"`{o['text']}` {o['direction']} NULLS {o['nulls']}" for o in ordering
        )
        lines.append(f"- **Ordering:** {ord_str}")
    else:
        lines.append("- **Ordering:** _(none)_")

    limit = schema.get("limit")
    lines.append(f"- **Limit:** {limit if limit is not None else '_(none)_'}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def explain_llm(
    data: UnqualifiedNode,
    session: PyDoughSession | None = None,
    *,
    format: str = "json",
) -> dict | str:
    """
    Returns a structured description of a PyDough collection expression,
    designed for LLM consumption.

    When ``format="json"`` (default) the return value is a JSON-serialisable
    dict.  On success the dict contains ``"error": false``, a
    ``"query_summary"`` sentence, an ordered ``"steps"`` list, and a
    ``"schema"`` summary.  On failure (qualification error, or expression
    rather than collection) the dict contains ``"error": true`` and a
    ``"message"`` with the error text; ``"steps"`` and ``"schema"`` are
    empty / null so the shape is always consistent.

    When ``format="md"`` the return value is a markdown string with the
    same information structured into ``## Query Summary``, ``## Steps``, and
    ``## Schema`` sections.  This format is easier for an LLM judge to read
    in a prompt.  Error payloads become a single ``## Error`` section.

    Args:
        `data`: an unqualified PyDough node (e.g. the result of evaluating a
          PyDough expression string).
        `session`: the PyDough session supplying graph metadata and active
          configuration.  Defaults to ``pydough.active_session``.
        `format`: ``"json"`` (default) for a JSON-serialisable dict, or
          ``"md"`` for a markdown string.

    Returns:
        A dict when ``format="json"``, or a ``str`` when ``format="md"``.

    Raises:
        ``ValueError`` if an unrecognised ``format`` value is passed.
    """
    if format not in ("json", "md"):
        raise ValueError(f"Unsupported format {format!r}. Use 'json' or 'md'.")

    if session is None:
        session = pydough.active_session

    # --- qualification -------------------------------------------------------
    qualified, error = qualify_safely(data, session)
    if error is not None:
        result = _error_payload(str(error))
        return _render_md(result) if format == "md" else result

    if isinstance(qualified, PyDoughExpressionQDAG):
        result = _error_payload(
            f"Expected a collection, but received an expression: "
            f"{qualified.to_string()}. Did you mean to use explain_term?"
        )
        return _render_md(result) if format == "md" else result

    assert isinstance(qualified, PyDoughCollectionQDAG)

    # --- build output --------------------------------------------------------
    steps = _collect_steps(qualified)
    schema = _build_schema(qualified)
    result = {
        "error": False,
        "query_summary": generate_query_summary(steps),
        "steps": steps,
        "schema": schema,
    }
    return _render_md(result) if format == "md" else result
