"""
Logic for applying decorrelation to hybrid trees before relational conversion
if the correlate is not a semi/anti join.
"""

__all__ = ["run_hybrid_decorrelation"]


from .hybrid_tree import (
    ConnectionType,
    HybridTree,
)


class Decorrelater:
    """
    TODO
    """

    def decorrelate_hybrid_tree(self, hybrid: HybridTree) -> HybridTree:
        """
        TODO
        """
        # Recursively decorrelate the ancestors of the current level of the
        # hybrid tree.
        if hybrid.parent is not None:
            hybrid._parent = self.decorrelate_hybrid_tree(hybrid.parent)
        # Iterate across all the children, identify any that are correlated,
        # and transform any of the correlated ones that require decorrelation
        # due to the type of connection.
        for idx, child in enumerate(hybrid.children):
            if idx not in hybrid.correlated_children:
                continue
            match child.connection_type:
                case (
                    ConnectionType.SINGULAR
                    | ConnectionType.AGGREGATION
                    | ConnectionType.SINGULAR_ONLY_MATCH
                    | ConnectionType.AGGREGATION_ONLY_MATCH
                    | ConnectionType.NDISTINCT
                    | ConnectionType.NDISTINCT_ONLY_MATCH
                ):
                    raise NotImplementedError(
                        f"PyDough does not yet support correlated references with the {child.connection_type.name} pattern."
                    )
                case (
                    ConnectionType.SEMI
                    | ConnectionType.ANTI
                    | ConnectionType.NO_MATCH_SINGULAR
                    | ConnectionType.NO_MATCH_AGGREGATION
                    | ConnectionType.NO_MATCH_NDISTINCT
                ):
                    # These patterns do not require decorrelation since they
                    # are supported via correlated SEMI/ANTI joins.
                    continue
        # Iterate across all the children and decorrelate them.
        for idx, child in enumerate(hybrid.children):
            hybrid.children[idx].subtree = self.decorrelate_hybrid_tree(child.subtree)
        return hybrid


def run_hybrid_decorrelation(hybrid: HybridTree) -> HybridTree:
    """
    TODO
    """
    decorr: Decorrelater = Decorrelater()
    return decorr.decorrelate_hybrid_tree(hybrid)
