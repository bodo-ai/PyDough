"""
TODO
"""

__all__ = ["choose_minimal_covering_set"]

from pydough.relational import RelationalExpression


def choose_minimal_covering_set(
    expressions: list[RelationalExpression],
    successful_idxs: list[int],
    heritage_tree: dict[RelationalExpression, set[RelationalExpression | None]],
) -> set[int]:
    """
    TODO: ADD DESCRIPTION
    """
    supported: set[RelationalExpression] = {expressions[idx] for idx in successful_idxs}
    not_needed: set[RelationalExpression] = set()
    include: set[RelationalExpression] = set()
    visited: set[RelationalExpression] = set()

    def traverse(expr: RelationalExpression):
        if expr in visited:
            return
        visited.add(expr)
        parents: set[RelationalExpression | None] = heritage_tree.get(expr, set())
        unnecessary: bool = True
        for parent in parents:
            if parent is not None:
                traverse(parent)
            if parent is None or (parent not in supported and parent not in not_needed):
                unnecessary = False
                if expr in supported:
                    include.add(expr)
        if unnecessary:
            not_needed.add(expr)

    for expr in expressions:
        traverse(expr)

    result: set[int] = {idx for idx in successful_idxs if expressions[idx] in include}
    return result
