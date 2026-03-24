"""
Implementation of the `pydough.explain` function, which provides detailed
explanations of PyDough unqualified nodes within the context of another PyDough
unqualified node.
"""

__all__ = ["explain_term", "find_unqualified_root"]


import pydough
import pydough.pydough_operators as pydop
from pydough.configs import PyDoughSession
from pydough.errors import PyDoughQDAGException
from pydough.pydough_operators.expression_operators import (
    SqlAliasExpressionFunctionOperator,
    SqlMacroExpressionFunctionOperator,
    SqlWindowAliasExpressionFunctionOperator,
)
from pydough.qdag import (
    BackReferenceExpression,
    ChildReferenceExpression,
    ColumnProperty,
    ExpressionFunctionCall,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    PyDoughQDAG,
    Reference,
    Singular,
    WindowCall,
)
from pydough.unqualified import (
    UnqualifiedAccess,
    UnqualifiedCalculate,
    UnqualifiedCross,
    UnqualifiedGeneratedCollection,
    UnqualifiedNode,
    UnqualifiedOrderBy,
    UnqualifiedPartition,
    UnqualifiedRoot,
    UnqualifiedSingular,
    UnqualifiedTopK,
    UnqualifiedWhere,
    display_raw,
    qualify_node,
    qualify_term,
)


def find_unqualified_root(node: UnqualifiedNode) -> UnqualifiedNode | None:
    """
    Recursively searches for the ancestor unqualified root of an unqualified
    node.

    Args:
        `node`: the node being searched for its underlying root node.

    Returns:
        The underlying root node if one can be found, otherwise None.
        For UnqualifiedRoot and UnqualifiedGeneratedCollection, returns the
        node itself. For chained nodes, walks the predecessor chain. Returns
        None for bare expressions or other rootless nodes.
    """
    match node:
        case UnqualifiedRoot() | UnqualifiedGeneratedCollection():
            return node
        case (
            UnqualifiedAccess()
            | UnqualifiedCalculate()
            | UnqualifiedWhere()
            | UnqualifiedOrderBy()
            | UnqualifiedTopK()
            | UnqualifiedPartition()
            | UnqualifiedCross()
            | UnqualifiedSingular()
        ):
            predecessor: UnqualifiedNode = node._parcel[0]
            return find_unqualified_root(predecessor)
        case _:
            return None


def collection_in_context_string(
    context: PyDoughCollectionQDAG, collection: PyDoughCollectionQDAG
) -> str:
    """
    Converts a collection in the context of another collection into a single
    string in a way that elides back collection references. For example,
    if the context is A.B.WHERE(C), and the collection is D.E, the result
    would be "A.B.WHERE(C).D.E".

    Args:
        `context`: the collection representing the context that `collection`
        exists within.
        `collection`: the collection that exists within `context`.

    Returns:
        The desired string representation of context and collection combined.
    """
    if (
        collection.preceding_context is not None
        and collection.preceding_context is not context
    ):
        return f"{collection_in_context_string(context, collection.preceding_context)}.{collection.standalone_string}"
    elif collection.ancestor_context == context:
        return f"{context.to_string()}.{collection.standalone_string}"
    else:
        assert collection.ancestor_context is not None
        return f"{collection_in_context_string(context, collection.ancestor_context)}.{collection.standalone_string}"


def explain_term(
    node: UnqualifiedNode,
    term: UnqualifiedNode,
    verbose: bool = False,
    session: PyDoughSession | None = None,
) -> str:
    """
    Displays information about an unqualified node as it exists within
    the context of an unqualified node. For example, if
    `explain_terms(Nations, name)` is called, it will display information about
    the `name` property of `Nations`. This information can include:
    - The structure of the qualified `collection` and `term`
    - Any additional children of the collection that must be derived in order
      to derive `term`.
    - The meaning of `term` within `collection`.
    - The cardinality of `term` within `collection`.
    - Examples of how to use `term` within `collection`.
    - How to learn more about `term`.

    Args:
        `node`: the unqualified node that, when qualified, becomes a collection
        that is used as the context through which `term` is derived.
        `term`: the unqualified node that information is being sought about.
        This term will only make sense if it is qualified within the context of
        `node`. This term could be an expression or a collection.
        `verbose`: if true, displays more detailed information about `node` and
        `term` in a less compact format.
        `config`: the PyDough session used for the explanation. If not provided,
        the active session will be used.

    Returns:
        An explanation of `term` as it exists within the context of `node`.
    """

    lines: list[str] = []
    root: UnqualifiedNode | None = find_unqualified_root(node)
    qualified_node: PyDoughQDAG | None = None
    if session is None:
        session = pydough.active_session
    try:
        if root is None:
            lines.append(
                f"Invalid first argument to pydough.explain_term: {display_raw(node)}"
            )
        else:
            qualified_node = qualify_node(node, session)
    except PyDoughQDAGException as e:
        if "Unrecognized term" in str(e):
            lines.append(
                f"Invalid first argument to pydough.explain_term: {display_raw(node)}"
                f"  {str(e)}"
                "This could mean you accessed a property using a name that does not exist, or\n"
                "that you need to place your PyDough code into a context for it to make sense."
            )
        else:
            raise e

    if isinstance(qualified_node, PyDoughExpressionQDAG):
        lines.append(
            "The first argument of pydough.explain_term is expected to be a collection, but"
        )
        lines.append("instead received the following expression:")
        lines.append(f" {qualified_node.to_string()}")
    elif qualified_node is not None and root is not None:
        assert isinstance(qualified_node, PyDoughCollectionQDAG)
        new_children, qualified_term = qualify_term(qualified_node, term, session)
        if verbose:
            lines.append("Collection:")
            for line in qualified_node.to_tree_string().splitlines():
                lines.append(f"  {line}")
        else:
            lines.append(f"Collection: {qualified_node.to_string()}")
        if isinstance(node, UnqualifiedCross):
            lines.append(
                f"Note: This collection is a CROSS product of"
                f" '{display_raw(node._parcel[0])}'"
                f" and '{display_raw(node._parcel[1])}'."
            )
        lines.append("")
        if len(new_children) > 0:
            lines.append(
                "The evaluation of this term first derives the following additional children to the collection before doing its main task:"
            )
            for idx, child in enumerate(new_children):
                if verbose:
                    lines.append(f"  child ${idx + 1}:")
                    for line in child.to_tree_string().splitlines()[1:]:
                        lines.append(f"  {line}")
                else:
                    lines.append(f"  child ${idx + 1}: {child.to_string()}")
            lines.append("")
        # If the qualification succeeded, dump info about the qualified node,
        # depending on what its nature is:
        if isinstance(qualified_term, PyDoughExpressionQDAG):
            lines.append(
                f"The term is the following expression: {qualified_term.to_string(True)}"
            )
            lines.append("")
            collection: PyDoughCollectionQDAG = qualified_node
            expr: PyDoughExpressionQDAG = qualified_term
            while True:
                match expr:
                    case ChildReferenceExpression():
                        lines.append(
                            f"This is a reference to expression '{expr.term_name}' of child ${expr.child_idx + 1}"
                        )
                        break
                    case BackReferenceExpression():
                        back_idx_str: str
                        match expr.back_levels % 10:
                            case 1:
                                back_idx_str = f"{expr.back_levels}st"
                            case 2:
                                back_idx_str = f"{expr.back_levels}nd"
                            case 3:
                                back_idx_str = f"{expr.back_levels}rd"
                            case _:
                                back_idx_str = f"{expr.back_levels}th"
                        lines.append(
                            f"This is a reference to expression '{expr.term_name}' of the {back_idx_str} ancestor of the collection, which is the following:"
                        )
                        if verbose:
                            for line in expr.ancestor.to_tree_string().splitlines():
                                lines.append(f"  {line}")
                        else:
                            lines.append(f"  {expr.ancestor.to_string()}")
                        break
                    case Reference():
                        expr = collection.get_expr(expr.term_name)
                        if (
                            isinstance(expr, Reference)
                            and collection.preceding_context is not None
                        ):
                            collection = collection.preceding_context
                    case ColumnProperty():
                        lines.append(
                            f"This is column '{expr.column_property.name}' of collection '{expr.column_property.collection.name}'"
                        )
                        break
                    case ExpressionFunctionCall():
                        if isinstance(
                            expr.operator,
                            SqlAliasExpressionFunctionOperator,
                        ):
                            suffix = (
                                ", aggregating them into a single value for each record of the collection"
                                if expr.operator.is_aggregation
                                else ""
                            )
                            lines.append(
                                f"This expression calls the user-defined function '{expr.operator.function_name}' on the following arguments{suffix}:"
                            )
                            for arg in expr.args:
                                assert isinstance(
                                    arg,
                                    (PyDoughCollectionQDAG, PyDoughExpressionQDAG),
                                )
                                lines.append(f"  {arg.to_string()}")
                            lines.append("")
                            if expr.operator.description is not None:
                                lines.append(
                                    f"Description: {expr.operator.description}"
                                )
                            lines.append(
                                f"This function is an alias for the SQL function '{expr.operator.sql_function_alias}'."
                            )
                            lines.append("")
                            lines.append(
                                "Call pydough.explain_term with this collection and any of the arguments to learn more about them."
                            )
                        elif isinstance(
                            expr.operator,
                            SqlMacroExpressionFunctionOperator,
                        ):
                            suffix = (
                                ", aggregating them into a single value for each record of the collection"
                                if expr.operator.is_aggregation
                                else ""
                            )
                            lines.append(
                                f"This expression calls the user-defined function '{expr.operator.function_name}' on the following arguments{suffix}:"
                            )
                            for arg in expr.args:
                                assert isinstance(
                                    arg,
                                    (PyDoughCollectionQDAG, PyDoughExpressionQDAG),
                                )
                                lines.append(f"  {arg.to_string()}")
                            lines.append("")
                            if expr.operator.description is not None:
                                lines.append(
                                    f"Description: {expr.operator.description}"
                                )
                            lines.append(
                                f"This function is defined by the SQL macro: '{expr.operator.macro_text}'."
                            )
                            if verbose:
                                dummies = [
                                    f"?{chr(ord('a') + i)}"
                                    for i in range(len(expr.args))
                                ]
                                dummy_list = ", ".join(f"'{d}'" for d in dummies)
                                expanded = expr.operator.macro_text.format(*dummies)
                                lines.append(
                                    f"Suppose this function were called on arguments"
                                    f" that are translated to the following in SQL: {dummy_list}"
                                )
                                lines.append(
                                    f"Then the final SQL text for this function call"
                                    f" would be: '{expanded}'"
                                )
                            lines.append("")
                            lines.append(
                                "Call pydough.explain_term with this collection and any of the arguments to learn more about them."
                            )
                        elif isinstance(expr.operator, pydop.BinaryOperator):
                            lines.append(
                                f"This expression combines the following arguments with the '{expr.operator.function_name}' operator:"
                            )
                            for arg in expr.args:
                                assert isinstance(
                                    arg,
                                    (PyDoughCollectionQDAG, PyDoughExpressionQDAG),
                                )
                                lines.append(f"  {arg.to_string()}")
                            lines.append("")
                            lines.append(
                                "Call pydough.explain_term with this collection and any of the arguments to learn more about them."
                            )
                        elif (
                            expr.operator in (pydop.COUNT, pydop.NDISTINCT)
                            and len(expr.args) == 1
                            and isinstance(expr.args[0], PyDoughCollectionQDAG)
                        ):
                            metric: str = (
                                "records"
                                if expr.operator == pydop.COUNT
                                else "distinct records"
                            )
                            lines.append(
                                f"This expression counts how many {metric} of the following subcollection exist for each record of the collection:"
                            )
                            for arg in expr.args:
                                assert isinstance(
                                    arg,
                                    (PyDoughCollectionQDAG, PyDoughExpressionQDAG),
                                )
                                lines.append(f"  {arg.to_string()}")
                            lines.append("")
                            lines.append(
                                "Call pydough.explain_term with this collection and any of the arguments to learn more about them."
                            )
                        elif (
                            expr.operator in (pydop.HAS, pydop.HASNOT)
                            and len(expr.args) == 1
                            and isinstance(expr.args[0], PyDoughCollectionQDAG)
                        ):
                            predicate: str = (
                                "has" if expr.operator == pydop.HAS else "does not have"
                            )
                            lines.append(
                                f"This expression returns whether the collection {predicate} any records of the following subcollection:"
                            )
                            for arg in expr.args:
                                assert isinstance(
                                    arg,
                                    (PyDoughCollectionQDAG, PyDoughExpressionQDAG),
                                )
                                lines.append(f"  {arg.to_string()}")
                            lines.append("")
                            lines.append(
                                "Call pydough.explain_term with this collection and any of the arguments to learn more about them."
                            )
                        else:
                            suffix = (
                                ", aggregating them into a single value for each record of the collection"
                                if expr.operator.is_aggregation
                                else ""
                            )
                            lines.append(
                                f"This expression calls the function '{expr.operator.function_name}' on the following arguments{suffix}:"
                            )
                            for arg in expr.args:
                                assert isinstance(
                                    arg,
                                    (PyDoughCollectionQDAG, PyDoughExpressionQDAG),
                                )
                                lines.append(f"  {arg.to_string()}")
                            lines.append("")
                            lines.append(
                                "Call pydough.explain_term with this collection and any of the arguments to learn more about them."
                            )
                        break
                    case WindowCall():
                        udf_window_op = (
                            expr.window_operator
                            if isinstance(
                                expr.window_operator,
                                SqlWindowAliasExpressionFunctionOperator,
                            )
                            else None
                        )
                        is_udf_window = udf_window_op is not None
                        kind = "user-defined window" if is_udf_window else "window"
                        func_name = expr.window_operator.function_name
                        if expr.args:
                            lines.append(
                                f"This expression calls the {kind} function {func_name!r} with the following arguments:"
                            )
                            for arg in expr.args:
                                lines.append(f"  {arg.to_string()}")
                        else:
                            lines.append(
                                f"This expression calls the {kind} function {func_name!r}."
                            )
                        lines.append("")
                        if expr.collation_args:
                            lines.append("Ordering (by):")
                            for arg in expr.collation_args:
                                lines.append(f"  {arg.to_string()}")
                            lines.append("")
                        if expr.levels is not None:
                            lines.append(f"Partition levels (per): {expr.levels}")
                            lines.append("")
                        if expr.kwargs:
                            lines.append("Additional options:")
                            for k, v in expr.kwargs.items():
                                match k:
                                    case "cumulative":
                                        desc = (
                                            "The window frame spans from "
                                            "the start of the partition to "
                                            "the current row."
                                            if v
                                            else "No cumulative window frame is applied."
                                        )
                                        lines.append(f"  cumulative={v!r}: {desc}")
                                    case "frame":
                                        assert isinstance(v, tuple) and len(v) == 2
                                        lower: int | None = v[0]
                                        upper: int | None = v[1]

                                        def _frame_bound(
                                            b: int | None, start: bool
                                        ) -> str:
                                            if b is None:
                                                return (
                                                    "the start of the partition"
                                                    if start
                                                    else "the end of the partition"
                                                )
                                            if b == 0:
                                                return "the current row"
                                            direction = "before" if b < 0 else "after"
                                            return f"{abs(b)} rows {direction} the current row"

                                        lines.append(
                                            f"  frame={v!r}: The window "
                                            f"frame spans from "
                                            f"{_frame_bound(lower, True)} "
                                            f"to {_frame_bound(upper, False)}."
                                        )
                                    case "default":
                                        lines.append(
                                            f"  default={v!r}: Returns "
                                            f"{v!r} when there are "
                                            f"insufficient rows in the "
                                            f"specified direction."
                                        )
                                    case "n_buckets":
                                        lines.append(
                                            f"  n_buckets={v!r}: The data "
                                            f"is divided into {v} "
                                            f"equal-sized buckets."
                                        )
                                    case "allow_ties":
                                        desc = (
                                            "Rows with equal ordering values "
                                            "receive the same rank."
                                            if v
                                            else "Each row receives a unique rank."
                                        )
                                        lines.append(f"  allow_ties={v!r}: {desc}")
                                    case "dense":
                                        desc = (
                                            "No gaps in ranking between tied "
                                            "groups (uses DENSE_RANK semantics)."
                                            if v
                                            else "Gaps are left in ranking after "
                                            "tied groups (uses RANK semantics)."
                                        )
                                        lines.append(f"  dense={v!r}: {desc}")
                                    case _:
                                        lines.append(f"  {k}: {v!r}")
                            lines.append("")
                        if udf_window_op is not None:
                            if udf_window_op.description is not None:
                                lines.append(
                                    f"Description: {udf_window_op.description}"
                                )
                            lines.append(
                                f"This function is an alias for the SQL window function '{udf_window_op.sql_function_alias}'."
                            )
                            order_str = (
                                "requires"
                                if udf_window_op.requires_order
                                else "does not require"
                            )
                            frame_str = (
                                "supports"
                                if udf_window_op.allows_frame
                                else "does not support"
                            )
                            lines.append(
                                f"This window function {order_str} ordering and {frame_str} frame specifications."
                            )
                            lines.append("")
                        lines.append(
                            "Call pydough.explain_term with this collection and any of the arguments to learn more about them."
                        )
                        break
                    case _:
                        raise NotImplementedError(expr.__class__.__name__)
            if verbose:
                lines.append("")
                if qualified_term.is_singular(qualified_node.starting_predecessor):
                    lines.append(
                        "This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection."
                    )
                    lines.append("For example, the following is valid:")
                    lines.append(
                        f"  {qualified_node.to_string()}.CALCULATE({qualified_term.to_string()})"
                    )
                else:
                    lines.append(
                        "This expression is plural with regards to the collection, meaning it can be placed in a CALCULATE of a collection if it is aggregated."
                    )
                    lines.append("For example, the following is valid:")
                    lines.append(
                        f"  {qualified_node.to_string()}.CALCULATE(COUNT({qualified_term.to_string()}))"
                    )
        else:
            assert isinstance(qualified_term, PyDoughCollectionQDAG)
            lines.append("The term is the following child of the collection:")
            if verbose:
                for line in qualified_term.to_tree_string().splitlines():
                    lines.append(f"  {line}")
            else:
                lines.append(f"  {qualified_term.to_string()}")
            if isinstance(qualified_term, Singular):
                lines.append("")
                lines.append(
                    "This child uses the SINGULAR operator, declaring the"
                    " following sub-collection as singular with respect to"
                    " the collection:"
                )
                if verbose:
                    for (
                        line
                    ) in qualified_term.preceding_context.to_tree_string().splitlines():
                        lines.append(f"  {line}")
                else:
                    lines.append(f"  {qualified_term.preceding_context.to_string()}")
            if verbose:
                lines.append("")
                assert len(qualified_term.calc_terms) > 0, (
                    "Child collection has no expression terms"
                )
                chosen_term_name: str = min(qualified_term.calc_terms)
                if qualified_term.is_singular(qualified_node.starting_predecessor):
                    lines.append(
                        "This child is singular with regards to the collection, meaning its scalar terms can be accessed by the collection as if they were scalar terms of the expression."
                    )
                    lines.append("For example, the following is valid:")
                    lines.append(
                        f"  {qualified_node.to_string()}.CALCULATE({qualified_term.to_string()}.{chosen_term_name})"
                    )
                else:
                    lines.append(
                        "This child is plural with regards to the collection, meaning its scalar terms can only be accessed by the collection if they are aggregated."
                    )
                    lines.append("For example, the following are valid:")
                    # TODO: when the collection is a CROSS, qualified_node.to_string()
                    # renders as e.g. "TPCH.nations.TPCH.regions" instead of the
                    # friendlier "TPCH.nations.CROSS(TPCH.regions)". Fixing this
                    # properly requires propagating CROSS identity through the QDAG
                    # and is deferred as a larger refactor.
                    # Or finding a way to render the unqualified node that
                    # looks nicer, like how QDAG looks
                    lines.append(
                        f"  {qualified_node.to_string()}.CALCULATE(COUNT({qualified_term.to_string()}.{chosen_term_name}))"
                    )
                    lines.append(
                        f"  {qualified_node.to_string()}.WHERE(HAS({qualified_term.to_string()}))"
                    )
                    lines.append(
                        f"  {qualified_node.to_string()}.ORDER_BY(COUNT({qualified_term.to_string()}).DESC())"
                    )
                lines.append("")
                lines.append(
                    "To learn more about this child, you can try calling pydough.explain on the following:"
                )
                lines.append(
                    f"  {collection_in_context_string(qualified_node, qualified_term)}"
                )

    return "\n".join(lines)
