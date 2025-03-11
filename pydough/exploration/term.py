"""
Implementation of the `pydough.explain` function, which provides detailed
explanations of PyDough unqualified nodes within the context of another PyDough
unqualified node.
"""

__all__ = ["explain_term", "find_unqualified_root"]


import pydough
import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.qdag import (
    BackReferenceExpression,
    ChildReferenceExpression,
    ColumnProperty,
    ExpressionFunctionCall,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    PyDoughQDAG,
    PyDoughQDAGException,
    Reference,
)
from pydough.unqualified import (
    UnqualifiedAccess,
    UnqualifiedCalculate,
    UnqualifiedNode,
    UnqualifiedOrderBy,
    UnqualifiedPartition,
    UnqualifiedRoot,
    UnqualifiedTopK,
    UnqualifiedWhere,
    display_raw,
    qualify_node,
    qualify_term,
)


def find_unqualified_root(node: UnqualifiedNode) -> UnqualifiedRoot | None:
    """
    Recursively searches for the ancestor unqualified root of an unqualified
    node.

    Args:
        `node`: the node being searched for its underlying root node.

    Returns:
        The underlying root node if one can be found, otherwise None.
    """
    match node:
        case UnqualifiedRoot():
            return node
        case (
            UnqualifiedAccess()
            | UnqualifiedCalculate()
            | UnqualifiedWhere()
            | UnqualifiedOrderBy()
            | UnqualifiedTopK()
            | UnqualifiedPartition()
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
    config: PyDoughConfigs | None = None,
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
        `config`: the configuration to use for the explanation. If not provided,
        the active session's configuration will be used.

    Returns:
        An explanation of `term` as it exists within the context of `node`.
    """

    lines: list[str] = []
    root: UnqualifiedRoot | None = find_unqualified_root(node)
    qualified_node: PyDoughQDAG | None = None
    if config is None:
        config = pydough.active_session.config
    try:
        if root is None:
            lines.append(
                f"Invalid first argument to pydough.explain_term: {display_raw(node)}"
            )
        else:
            qualified_node = qualify_node(node, root._parcel[0], config)
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
        new_children, qualified_term = qualify_term(
            qualified_node, term, root._parcel[0]
        )
        if verbose:
            lines.append("Collection:")
            for line in qualified_node.to_tree_string().splitlines():
                lines.append(f"  {line}")
        else:
            lines.append(f"Collection: {qualified_node.to_string()}")
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
                                back_idx_str = f"{expr.back_levels}2nd"
                            case 3:
                                back_idx_str = f"{expr.back_levels}3rd"
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
                        if isinstance(expr.operator, pydop.BinaryOperator):
                            lines.append(
                                f"This expression combines the following arguments with the '{expr.operator.function_name}' operator:"
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
                                arg, (PyDoughCollectionQDAG, PyDoughExpressionQDAG)
                            )
                            lines.append(f"  {arg.to_string()}")
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
            if verbose:
                lines.append("")
                assert len(qualified_term.calc_terms) > 0, (
                    "Child collection has no expression terms"
                )
                chosen_term_name: str = min(qualified_term.calc_terms)
                if qualified_term.starting_predecessor.is_singular(
                    qualified_node.starting_predecessor
                ):
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
