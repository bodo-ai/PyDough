"""
Definitions of the hybrid operations abstract base class and its concrete
subclasses. These represent the steps taken within the pipeline of each
hybrid tree, such as accessing a collection, applying a filter, or
deriving new columns.
"""

__all__ = [
    "HybridCalculate",
    "HybridChildPullUp",
    "HybridCollectionAccess",
    "HybridFilter",
    "HybridLimit",
    "HybridNoop",
    "HybridOperation",
    "HybridPartition",
    "HybridPartitionChild",
    "HybridRoot",
]


from typing import TYPE_CHECKING

import pydough.pydough_operators as pydop
from pydough.qdag import (
    CollectionAccess,
    ColumnProperty,
    PyDoughExpressionQDAG,
)

from .hybrid_connection import HybridConnection
from .hybrid_expressions import (
    HybridChildRefExpr,
    HybridCollation,
    HybridColumnExpr,
    HybridExpr,
    HybridFunctionExpr,
    HybridRefExpr,
)

if TYPE_CHECKING:
    from .hybrid_tree import HybridTree


class HybridOperation:
    """
    Base class for an operation done within a pipeline of a HybridTree, such
    as a filter or table collection access. Every such class contains the
    following:
    - `terms`: mapping of names to expressions accessible from that point in
               the pipeline execution.
    - `renamings`: mapping of names to a new name that should be used to access
               them from within `terms`. This is used when a `CALCULATE`
               overrides a term name so that future invocations of the term
               name use the renamed version, while key operations like joins
               can still access the original version.
    - `orderings`: list of collation expressions that specify the order
               that a hybrid operation is sorted by.
    - `unique_exprs`: list of expressions that are used to uniquely identify
               records within the current level of the hybrid tree.
    - `is_hidden`: whether the operation is hidden when converting the tree
               to a string.
    """

    def __init__(
        self,
        terms: dict[str, HybridExpr],
        renamings: dict[str, str],
        orderings: list[HybridCollation],
        unique_exprs: list[HybridExpr],
    ):
        self.terms: dict[str, HybridExpr] = terms
        self.renamings: dict[str, str] = renamings
        self.orderings: list[HybridCollation] = orderings
        self.unique_exprs: list[HybridExpr] = unique_exprs
        self.is_hidden: bool = False

    def __eq__(self, other):
        return type(self) is type(other) and repr(self) == repr(other)

    def search_term_definition(self, name: str) -> HybridExpr | None:
        return self.terms.get(name, None)

    def replace_expressions(
        self,
        replacements: dict[HybridExpr, HybridExpr],
    ) -> None:
        """
        Replaces expressions within the operation with values from the provided
        replacement mapping.

        Args:
            `replacements`: a dictionary mapping expressions that should be
            replaced to their replacements.
        """
        for term_name, term in self.terms.items():
            new_term: HybridExpr = term.replace_expressions(replacements)
            self.terms[term_name] = new_term

    def get_term_as_ref(self, name: str) -> HybridRefExpr:
        """
        Fetches a term from the operation, coercing it to a HybridRefExpr.

        Args:
            `name`: the name of the desired reference.

        Returns:
            A HybridRefExpr corresponding to the term with the desired name.
        """
        term: HybridExpr | None = self.terms.get(name, None)
        if term is None:
            raise ValueError(f"Term not found: {name}")
        if isinstance(term, HybridRefExpr):
            return term
        if isinstance(term, HybridColumnExpr):
            return HybridRefExpr(name, term.typ)
        raise ValueError(f"Term cannot be coerced to a reference: {name}")


class HybridRoot(HybridOperation):
    """
    Class for HybridOperation corresponding to the "root" context.
    """

    def __init__(self):
        super().__init__({}, {}, [], [])

    def __repr__(self):
        return "ROOT"


class HybridCollectionAccess(HybridOperation):
    """
    Class for HybridOperation corresponding to accessing a collection (either
    directly or as a subcollection).
    """

    def __init__(self, collection: CollectionAccess):
        """
        Args:
            `collection`: the QDAG node for the collection access being
            represented by the hybrid operation.
        """
        self.collection: CollectionAccess = collection
        terms: dict[str, HybridExpr] = {}
        for name in collection.calc_terms:
            raw_expr = collection.get_term_from_property(name)
            assert isinstance(raw_expr, ColumnProperty)
            terms[name] = HybridColumnExpr(raw_expr)
        unique_exprs: list[HybridExpr] = []
        for name in sorted(collection.unique_terms, key=str):
            expr: PyDoughExpressionQDAG = collection.get_expr(name)
            unique_exprs.append(HybridRefExpr(name, expr.pydough_type))
        self.general_condition: HybridExpr | None = None
        super().__init__(terms, {}, [], unique_exprs)

    def __repr__(self):
        return f"COLLECTION[{self.collection.name}]"


class HybridPartitionChild(HybridOperation):
    """
    Class for HybridOperation corresponding to accessing the data of a
    PARTITION as a child.
    """

    def __init__(self, subtree: "HybridTree"):
        """
        Args:
            `subtree`: the hybrid tree representing the data originally being
            partitioned.
        """
        self.subtree: HybridTree = subtree
        super().__init__(
            subtree.pipeline[-1].terms,
            subtree.pipeline[-1].renamings,
            subtree.pipeline[-1].orderings,
            subtree.pipeline[-1].unique_exprs,
        )

    def __repr__(self):
        return repr(self.subtree)


class HybridCalculate(HybridOperation):
    """
    Class for HybridOperation corresponding to a CALCULATE operation.
    """

    def __init__(
        self,
        predecessor: HybridOperation,
        new_expressions: dict[str, HybridExpr],
        orderings: list[HybridCollation],
    ):
        """
        Args:
            `predecessor`: the HybridOperation that precedes hte new CALCULATE
            operation in the pipeline.
            `new_expressions`: a mapping of new expression names to their
            corresponding expressions.
            `orderings`: a list of collation expressions that specify the order
            that a hybrid operation is sorted by.
        """
        self.predecessor: HybridOperation = predecessor
        terms: dict[str, HybridExpr] = {}
        renamings: dict[str, str] = {}
        for name, expr in predecessor.terms.items():
            terms[name] = HybridRefExpr(name, expr.typ)
        renamings.update(predecessor.renamings)
        new_renamings: dict[str, str] = {}
        for name, expr in new_expressions.items():
            if name in terms and terms[name] == expr:
                continue
            expr = expr.apply_renamings(predecessor.renamings)
            used_name: str = name
            idx: int = 0
            while (
                used_name in terms
                or used_name in renamings
                or used_name in new_renamings
            ):
                if (
                    (used_name not in renamings)
                    and (used_name not in new_renamings)
                    and (self.predecessor.search_term_definition(used_name) == expr)
                ):
                    break
                used_name = f"{name}_{idx}"
                idx += 1
                new_renamings[name] = used_name
            terms[used_name] = expr
        renamings.update(new_renamings)
        for old_name, new_name in new_renamings.items():
            expr = new_expressions.pop(old_name)
            new_expressions[new_name] = expr
        super().__init__(terms, renamings, orderings, predecessor.unique_exprs)
        self.new_expressions = new_expressions

    def __repr__(self):
        return f"CALCULATE[{self.new_expressions}]"

    def search_term_definition(self, name: str) -> HybridExpr | None:
        if name in self.new_expressions:
            expr: HybridExpr = self.new_expressions[name]
            if not (isinstance(expr, HybridRefExpr) and expr.name == name):
                return self.new_expressions[name]
        return self.predecessor.search_term_definition(name)

    def replace_expressions(
        self,
        replacements: dict[HybridExpr, HybridExpr],
    ) -> None:
        for term_name, term in self.terms.items():
            new_term: HybridExpr = term.replace_expressions(replacements)
            self.terms[term_name] = new_term
            if term_name in self.new_expressions:
                self.new_expressions[term_name] = new_term


class HybridFilter(HybridOperation):
    """
    Class for HybridOperation corresponding to a WHERE operation.
    """

    def __init__(self, predecessor: HybridOperation, condition: HybridExpr):
        """
        Args:
            `predecessor`: the HybridOperation that precedes the new FILTER
            operation in the pipeline.
            `condition`: the expression that is used to filter the data.
        """
        super().__init__(
            predecessor.terms,
            predecessor.renamings,
            predecessor.orderings,
            predecessor.unique_exprs,
        )
        self.predecessor: HybridOperation = predecessor
        self.condition: HybridExpr = condition

    def __repr__(self):
        return f"FILTER[{self.condition}]"

    def search_term_definition(self, name: str) -> HybridExpr | None:
        return self.predecessor.search_term_definition(name)

    def replace_expressions(
        self,
        replacements: dict[HybridExpr, HybridExpr],
    ) -> None:
        self.condition = self.condition.replace_expressions(replacements)


class HybridChildPullUp(HybridOperation):
    """
    Class for HybridOperation corresponding to evaluating all of the logic from
    a child subtree of the current pipeline then treating it as the current
    level.
    """

    def __init__(
        self,
        hybrid: "HybridTree",
        child_idx: int,
        original_child_height: int,
    ):
        """
        Args:
            `hybrid`: the hybrid tree that is being modified by having one of
            its children pulled up.
            `child_idx`: the index of the child subtree to pull up, which
            should refer to a child that was just de-correlated.
            `original_child_height`: the height of the child subtree in the
                hybrid tree before it was de-correlated.
        """
        self.child: HybridConnection = hybrid.children[child_idx]
        self.child_idx: int = child_idx
        self.pullup_remapping: dict[HybridExpr, HybridExpr] = {}

        # Find the level from the child tree that is the equivalent of the
        # level from the child tree that is being replaced.
        current_level: HybridTree = self.child.subtree
        for _ in range(original_child_height):
            assert current_level.parent is not None
            current_level = current_level.parent

        # Snapshot the renamings from the current level, and use its unique
        # terms as the unique terms for this level.
        renamings: dict[str, str] = current_level.pipeline[-1].renamings
        unique_exprs: list[HybridExpr] = []
        for unique_expr in current_level.pipeline[-1].unique_exprs:
            unique_exprs.append(unique_expr.shift_back(original_child_height))

        # Start by adding terms from the bottom level of the child as child ref
        # expressions accessible from the parent.
        terms: dict[str, HybridExpr] = {}
        for term_name, term_expr in current_level.pipeline[-1].terms.items():
            child_ref: HybridChildRefExpr = HybridChildRefExpr(
                term_name, child_idx, term_expr.typ
            )
            terms[term_name] = child_ref

        # Iterate through the level identified earlier & its ancestors to find
        # all of their terms and add them to the parent via accesses to
        # backreferences from the child. These terms are placed in the pullup
        # remapping dictionary so to provide hints on how to translate
        # expressions with regards to the parent level into lookups from within
        # the child subtree.
        extra_height: int = 0
        agg_idx: int = 0
        while True:
            current_terms: dict[str, HybridExpr] = current_level.pipeline[-1].terms
            for term_name in sorted(current_terms):
                # Identify the expression that is being accessed from one of
                # the levels of the child subtree.
                current_expr: HybridExpr = HybridRefExpr(
                    term_name, current_terms[term_name].typ
                ).shift_back(extra_height)
                back_expr: HybridExpr = current_expr.shift_back(original_child_height)
                if self.child.connection_type.is_aggregation:
                    # If aggregating, wrap the backreference in an ANYTHING
                    # call that is added to the agg calls list so it can be
                    # passed through the aggregation.
                    passthrough_agg: HybridFunctionExpr = HybridFunctionExpr(
                        pydop.ANYTHING, [back_expr], back_expr.typ
                    )
                    agg_name: str
                    # If the aggregation already exists, use it. Otherwise
                    # insert a new aggregation.
                    if passthrough_agg in self.child.aggs.values():
                        agg_name = self.child.fetch_agg_name(passthrough_agg)
                    else:
                        agg_name = f"agg_{agg_idx}"
                        while (
                            agg_name in self.child.aggs
                            or agg_name in self.child.subtree.pipeline[-1].terms
                        ):
                            agg_idx += 1
                            agg_name = f"agg_{agg_idx}"
                        self.child.aggs[agg_name] = passthrough_agg
                        self.pullup_remapping[current_expr] = HybridRefExpr(
                            agg_name, back_expr.typ
                        )
                else:
                    # Otherwise, add an access to the backreference to the
                    # pullup remapping.
                    self.pullup_remapping[current_expr] = back_expr
            if current_level.parent is None:
                break
            current_level = current_level.parent
            extra_height += 1

        if self.child.connection_type.is_aggregation:
            agg_keys: list[HybridExpr] | None = self.child.subtree.agg_keys
            if agg_keys is not None:
                for agg_key in agg_keys:
                    if isinstance(agg_key, HybridRefExpr):
                        self.pullup_remapping[agg_key] = agg_key

        super().__init__(terms, renamings, [], unique_exprs)

    def __repr__(self):
        return f"PULLUP[${self.child_idx}: {self.pullup_remapping}]"


class HybridNoop(HybridOperation):
    """
    Class for HybridOperation corresponding to a no-op.
    """

    def __init__(self, last_operation: HybridOperation):
        """
        Args:
            `last_operation`: the last HybridOperation in the pipeline that
            the no-op builds on top of.
        """
        super().__init__(
            last_operation.terms,
            last_operation.renamings,
            last_operation.orderings,
            last_operation.unique_exprs,
        )

    def __repr__(self):
        return "NOOP"


class HybridPartition(HybridOperation):
    """
    Class for HybridOperation corresponding to a PARTITION operation.
    """

    def __init__(self):
        super().__init__({}, {}, [], [])
        self.key_names: list[str] = []

    def __repr__(self):
        key_map = {name: self.terms[name] for name in self.key_names}
        return f"PARTITION[{key_map}]"

    def add_key(self, key_name: str, key_expr: HybridExpr) -> None:
        """
        Adds a new key to the HybridPartition.

        Args:
            `key_name`: the name of the partitioning key.
            `key_expr`: the expression used to partition.
        """
        self.key_names.append(key_name)
        self.terms[key_name] = key_expr
        self.unique_exprs.append(HybridRefExpr(key_name, key_expr.typ))


class HybridLimit(HybridOperation):
    """
    Class for HybridOperation corresponding to a TOP K operation.
    """

    def __init__(
        self,
        predecessor: HybridOperation,
        records_to_keep: int,
    ):
        """
        Args:
            `predecessor`: the HybridOperation that precedes the new LIMIT
            operation in the pipeline.
            `records_to_keep`: the number of records to keep in the LIMIT
            operation.
        """
        super().__init__(
            predecessor.terms,
            predecessor.renamings,
            predecessor.orderings,
            predecessor.unique_exprs,
        )
        self.predecessor: HybridOperation = predecessor
        self.records_to_keep: int = records_to_keep

    def __repr__(self):
        return f"LIMIT_{self.records_to_keep}[{self.orderings}]"

    def search_term_definition(self, name: str) -> HybridExpr | None:
        return self.predecessor.search_term_definition(name)
