"""
Logic for removing repeated sort keys from the orderings of `RelationalRoot`
and `Limit` nodes.

Both constructors already remove repeated sort keys, but several optimization
passes rewrite these orderings in place after the nodes are built (projection
merging, projection pullup, column bubbling and expression simplification).
Those rewrites can turn different sort keys into the same expression, so this
pass runs once all of them are done.
"""

__all__ = ["deduplicate_orderings"]


from pydough.relational import (
    ExpressionSortInfo,
    Limit,
    RelationalNode,
    RelationalRoot,
)


def deduplicate_orderings(node: RelationalNode) -> None:
    """
    Removes repeated sort keys, in place, from the orderings of every root and
    limit node in the tree rooted at `node`.

    Args:
        `node`: The root of the relational tree to transform.
    """
    if isinstance(node, (RelationalRoot, Limit)):
        node._orderings = ExpressionSortInfo.deduplicate(node.orderings)
    for child in node.inputs:
        deduplicate_orderings(child)
