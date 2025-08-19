"""
Definitions for the information of how to connect a hybrid tree to a child
hybrid tree, including the dataclass for the connection and the enum it uses
to classify the type of connection.
"""

__all__ = ["ConnectionType", "HybridConnection"]

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from pydough.relational import JoinType

from .hybrid_expressions import (
    HybridFunctionExpr,
)

if TYPE_CHECKING:
    from .hybrid_tree import HybridTree


class ConnectionType(Enum):
    """
    An enum describing how a hybrid tree is connected to a child tree.
    """

    SINGULAR = 0
    """
    The child should be 1:1 with regards to the parent, and can thus be
    accessed via a simple left join without having to worry about cardinality
    contamination.
    """

    AGGREGATION = 1
    """
    The child is being accessed for the purposes of aggregating its columns.
    The aggregation is done on top of the translated subtree before it is
    combined with the parent tree via a left join. The aggregate call may be
    augmented after the left join, e.g. to coalesce with a default value if the
    left join was not used. The grouping keys for the aggregate are the keys
    used to join the parent tree output onto the subtree output.

    If this is used as a child access of a `PARTITION` node, there is no left
    join, though some of the post-processing steps may still occur.
    """

    NDISTINCT = 2
    """
    The child is being accessed for the purposes of counting how many
    distinct elements it has. This is implemented by grouping the child subtree
    on both the original grouping keys as well as the unique columns of the
    subcollection without any aggregations, then having the `aggs` list contain
    a solitary `COUNT` term before being left-joined. The result is coalesced
    with 0, unless this is used as a child access of a `PARTITION` node.
    """

    SEMI = 3
    """
    The child is being used as a semi-join.
    """

    SINGULAR_ONLY_MATCH = 4
    """
    If a SINGULAR connection overlaps with a SEMI connection, then they are
    fused into a variant of SINGULAR that can use an INNER join instead of a
    LEFT join.
    """

    AGGREGATION_ONLY_MATCH = 5
    """
    If an AGGREGATION connection overlaps with a SEMI connection, then they are
    fused into a variant of AGGREGATION that can use an INNER join instead of a
    LEFT join.
    """

    NDISTINCT_ONLY_MATCH = 6
    """
    If a NDISTINCT connection overlaps with a SEMI connection, then they are
    fused into a variant of NDISTINCT that can use an INNER join instead of a
    LEFT join.
    """

    ANTI = 7
    """
    The child is being used as an anti-join.
    """

    NO_MATCH_SINGULAR = 8
    """
    If a SINGULAR connection overlaps with an ANTI connection, then it
    becomes this connection which still functions as an ANTI but replaces
    all of the child references with NULL.
    """

    NO_MATCH_AGGREGATION = 9
    """
    If an AGGREGATION connection overlaps with an ANTI connection, then it
    becomes this connection which still functions as an ANTI but replaces
    all of the aggregation outputs with NULL.
    """

    NO_MATCH_NDISTINCT = 10
    """
    If a NDISTINCT connection overlaps with an ANTI connection, then it
    becomes this connection which still functions as an ANTI but replaces
    the NDISTINCT output with 0.
    """

    @property
    def is_singular(self) -> bool:
        """
        Whether the connection type corresponds to one of the 3 SINGULAR
        cases.
        """
        return self in (
            ConnectionType.SINGULAR,
            ConnectionType.SINGULAR_ONLY_MATCH,
            ConnectionType.NO_MATCH_SINGULAR,
        )

    @property
    def is_aggregation(self) -> bool:
        """
        Whether the connection type corresponds to one of the 3 AGGREGATION
        cases.
        """
        return self in (
            ConnectionType.AGGREGATION,
            ConnectionType.AGGREGATION_ONLY_MATCH,
            ConnectionType.NO_MATCH_AGGREGATION,
        )

    @property
    def is_ndistinct(self) -> bool:
        """
        Whether the connection type corresponds to one of the 3 NDISTINCT
        cases.
        """
        return self in (
            ConnectionType.NDISTINCT,
            ConnectionType.NDISTINCT_ONLY_MATCH,
            ConnectionType.NO_MATCH_NDISTINCT,
        )

    @property
    def is_semi(self) -> bool:
        """
        Whether the connection type corresponds to one of the 4 SEMI cases.
        """
        return self in (
            ConnectionType.SEMI,
            ConnectionType.SINGULAR_ONLY_MATCH,
            ConnectionType.AGGREGATION_ONLY_MATCH,
            ConnectionType.NDISTINCT_ONLY_MATCH,
        )

    @property
    def is_anti(self) -> bool:
        """
        Whether the connection type corresponds to one of the 4 ANTI cases.
        """
        return self in (
            ConnectionType.ANTI,
            ConnectionType.NO_MATCH_SINGULAR,
            ConnectionType.NO_MATCH_AGGREGATION,
            ConnectionType.NO_MATCH_AGGREGATION,
        )

    @property
    def is_neutral_matching(self) -> bool:
        """
        Whether the connection type is neutral with regards to how it accesses
        any child terms.
        """
        return self in (ConnectionType.SEMI, ConnectionType.ANTI)

    @property
    def is_singular_compatible(self) -> bool:
        """
        Whether the connection type can be reconciled with SINGULAR.
        """
        return self.is_singular or self.is_neutral_matching

    @property
    def is_aggregation_compatible(self) -> bool:
        """
        Whether the connection type can be reconciled with AGGREGATION.
        """
        return self.is_aggregation or self.is_neutral_matching

    @property
    def is_ndistinct_compatible(self) -> bool:
        """
        Whether the connection type can be reconciled with NDISTINCT.
        """
        return self.is_ndistinct or self.is_neutral_matching

    @property
    def join_type(self) -> JoinType:
        """
        The type of join that the connection type corresponds to.
        """
        match self:
            case (
                ConnectionType.SINGULAR
                | ConnectionType.AGGREGATION
                | ConnectionType.NDISTINCT
            ):
                # A regular connection without SEMI or ANTI has to be a LEFT
                # join since parent records without subcollection instances
                # must be maintained.
                return JoinType.LEFT
            case (
                ConnectionType.SINGULAR_ONLY_MATCH
                | ConnectionType.AGGREGATION_ONLY_MATCH
                | ConnectionType.NDISTINCT_ONLY_MATCH
            ):
                # A regular connection combined with SEMI can be an INNER join
                # since records without matches can be dropped.
                return JoinType.INNER
            case ConnectionType.SEMI:
                # A standalone SEMI connection just becomes a SEMI join.
                return JoinType.SEMI
            case (
                ConnectionType.ANTI
                | ConnectionType.NO_MATCH_SINGULAR
                | ConnectionType.NO_MATCH_AGGREGATION
                | ConnectionType.NO_MATCH_NDISTINCT
            ):
                # Any type of ANTI connection just becomes an ANTI join; the
                # relational conversion step is responsible for converting any
                # references to the child expressions/aggregations to NULL
                # since they do not exist.
                return JoinType.ANTI
            case _:
                raise ValueError(f"Connection type {self} does not have a join type")

    def reconcile_connection_types(self, other: "ConnectionType") -> "ConnectionType":
        """
        Combines two connection types and returns the resulting connection
        type used when they overlap.

        Args:
            `other`: the other connection type that is to be reconciled
            with `self`.

        Returns:
            The connection type produced when `self` and `other` overlap.
        """
        # For duplicates, the connection type is unmodified
        if self == other:
            return self

        # Determine whether the connection types are being reconciled into
        # a combination that keeps matches or drops matches (has to be
        # exactly one of these).
        either_semi: bool = self.is_semi or other.is_semi
        either_anti: bool = self.is_anti or other.is_anti
        only_match: bool
        if either_semi and not either_anti:
            only_match = True
        elif either_anti and not either_semi:
            only_match = False
        else:
            raise ValueError(
                f"Malformed or unsupported combination of connection types: {self} and {other}"
            )

        # Determine if the connection types are being resolved into a SINGULAR
        # combination.
        if self.is_singular_compatible and other.is_singular_compatible:
            if only_match:
                return ConnectionType.SINGULAR_ONLY_MATCH
            else:
                return ConnectionType.NO_MATCH_SINGULAR

        # Determine if the connection types are being resolved into an
        # AGGREGATION combination.
        if self.is_aggregation_compatible and other.is_aggregation_compatible:
            if only_match:
                return ConnectionType.AGGREGATION_ONLY_MATCH
            else:
                return ConnectionType.NO_MATCH_AGGREGATION

        # Determine if the connection types are being resolved into a NDISTINCT
        # combination.
        if self.is_ndistinct_compatible and other.is_ndistinct_compatible:
            if only_match:
                return ConnectionType.NDISTINCT_ONLY_MATCH
            else:
                return ConnectionType.NO_MATCH_NDISTINCT

        # Every other combination is malformed
        raise ValueError(
            f"Malformed combination of connection types: {self} and {other}"
        )


@dataclass
class HybridConnection:
    """
    Parcel class corresponding to information about one of the children
    of a HybridTree. Contains the following information:
    - `parent`: the HybridTree that the connection exists within.
    - `subtree`: the HybridTree corresponding to the child itself, starting
      from the bottom.
    - `connection_type`: an enum indicating which connection type is being
       used.
    - `min_steps`: an index indicating the last step in the pipeline
       that must be completed before the child can be defined (inclusive).
    - `max_steps`: a index indicating the maximum step in the pipeline that the
       child can be defined at (exclusive).
    - `aggs`: a mapping of aggregation calls made onto expressions relative to the
       context of `subtree`.
    """

    parent: "HybridTree"
    """
    The HybridTree that the connection exists within.
    """

    subtree: "HybridTree"
    """
    The HybridTree corresponding to the child itself, starting from the bottom.
    """

    connection_type: ConnectionType
    """
    The type of connection that the child subtree is being accessed with
    relative to the parent hybrid tree.
    """

    min_steps: int
    """
    The step in the parent pipeline that this connection MUST be defined after.
    """

    max_steps: int
    """
    The step in the parent pipeline that this connection MUST be defined before.
    """

    aggs: dict[str, HybridFunctionExpr]
    """
    A dictionary storing information about how to aggregate the child before
    joining it to the parent. The keys are the names of the aggregation calls
    within the child subtree, and the values are the corresponding aggregation
    expressions defined relative to the child subtree.
    """

    always_exists: bool | None = None
    """
    Whether the connection is guaranteed to have at least one matching
    record for every parent record. If None, this is unknown and must be
    inferred from the subtree (can be stored and then modified later).
    """

    def get_always_exists(self) -> bool:
        """
        Returns whether the connection is guaranteed to have at least one
        matching record for every parent record.
        """
        if self.always_exists is None:
            self.always_exists = self.subtree.always_exists()
        return self.always_exists

    def __eq__(self, other):
        return (
            self.connection_type == other.connection_type
            and self.subtree == other.subtree
        )

    def fetch_agg_name(self, call: HybridFunctionExpr) -> str:
        """
        Returns the name of an aggregation call within the connection. Throws
        an error if the aggregation call is not found.

        Args:
            `call`: The aggregation call whose name within the child connection
            is being sought.

        Returns:
            The string `name` such that `self.aggs[name]` returns call.

        Raises:
            `ValueError`: if the aggregation call is not found.
        """
        try:
            return next(name for name, agg in self.aggs.items() if agg == call)
        except StopIteration:
            raise ValueError(f"Aggregation call {call} not found in {self.aggs}")
