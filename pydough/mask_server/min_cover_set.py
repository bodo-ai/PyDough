"""
Logic for choosing the minimal set of expressions out of a list such that only
expressions marked as "successful" are included, and every expression from the
list is either included or has an ancestor that is included.
"""

__all__ = ["choose_minimal_covering_set"]

from pydough.relational import RelationalExpression


def choose_minimal_covering_set(
    expressions: list[RelationalExpression],
    successful_idxs: list[int],
    heritage_tree: dict[RelationalExpression, set[RelationalExpression | None]],
) -> set[int]:
    """
    Identifies the minimal set of indices from `successful_idxs` such that every
    expression in `expressions` is either included in the set or has an ancestor
    that is included.

    Args:
        `expressions`: The list of expressions to cover.
        `successful_idxs`: The list of indices into `expressions` that are
        marked as successful.
        `heritage_tree`: A mapping of each expression to its set of parent
        expressions in the relational tree. `None` is also included in the set
        if the expression ever appears standalone (i.e., as the root of a
        relational expression in the tree). Each expression maps to a set since
        an expression can appear in multiple places within the relational tree.

    Returns:
        The set of indices from `successful_idxs` that form the minimal covering
        set.
    """

    # Build the following datastructures:
    # 1. Set of expressions that are marked as successful.
    # 2. Set of expressions that are not needed (i.e., every ancestor is either
    # included in the answer, or is also not needed).
    # 3. Set of expressions to include in the final answer.
    # 4. Set of expressions already visited during traversal (to ensure dynamic
    # programming principles are upheld to avoid redundant work).
    supported: set[RelationalExpression] = {expressions[idx] for idx in successful_idxs}
    not_needed: set[RelationalExpression] = set()
    include: set[RelationalExpression] = set()
    visited: set[RelationalExpression] = set()

    # Run a DFS traversal for each expression, walking through the full forest
    # from `expressions`.
    def traverse(expr: RelationalExpression):
        # Abort if already visited, then mark the node as visited.
        if expr in visited:
            return
        visited.add(expr)

        # Extract all parents of the expression from the heritage tree. A
        # `None` parent indicates that the current expression appears
        # standalone. For each non-None parent, traverse it recursively. The
        # expression starts out as unecessary, but loses that distinction if
        # any of the parents indicate otherwise.
        parents: set[RelationalExpression | None] = heritage_tree.get(expr, {None})
        unnecessary: bool = True
        for parent in parents:
            if parent is not None:
                traverse(parent)

            # The expression loses its unecessary distinction if it appears
            # standalone, or if any of its parents are simultaneously
            # unsupported and necessary.
            if parent is None or (parent not in supported and parent not in not_needed):
                unnecessary = False
                # If the current expression loses the unnecessary distinction,
                # add it in the inclusion set, but only if it is supported.
                if expr in supported:
                    include.add(expr)

        # If the expression was marked as unnecessary, add it to the
        # `not_needed` set.
        if unnecessary:
            not_needed.add(expr)

    for expr in expressions:
        traverse(expr)

    # Return the set of indices from `successful_idxs` that correspond to
    # expressions that were placed in `include` during the DFS forest run.
    result: set[int] = {idx for idx in successful_idxs if expressions[idx] in include}
    return result
