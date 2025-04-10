"""
Logic used to partially transpose aggregates beneath joins when splittable into
a partial aggregation.
"""

__all__ = ["remove_redundant_aggs"]


import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    EmptySingleton,
    Filter,
    Join,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    Scan,
)
from pydough.relational.rel_util import (
    bubble_uniqueness,
    extract_equijoin_keys,
)
from pydough.types import BooleanType, Int64Type


def delete_aggregation(agg: Aggregate) -> RelationalNode:
    """
    Deletes an aggregation node from the relational plan determined to be
    redundant via uniqueness, and returns the equivalent projection node.

    Args:
        `agg`: the aggregation node to be deleted.

    Returns:
        The transformed node.
    """
    projection_cols: dict[str, RelationalExpression] = {}
    for name, col in agg.keys.items():
        projection_cols[name] = col
    for name, agg_call in agg.aggregations.items():
        match agg_call.op:
            # For these operators, when called on a single row, just return
            # that row unmodified.
            case (
                pydop.SUM
                | pydop.MIN
                | pydop.MAX
                | pydop.AVG
                | pydop.ANYTHING
                | pydop.MEDIAN
            ):
                projection_cols[name] = agg_call.inputs[0]
            # If COUNT is called with no arguments, always return 1. If it is
            # called with 1 argument, return 1 if the row is non-null,
            # otherwise 0.
            case pydop.COUNT:
                if len(agg_call.inputs) == 0:
                    projection_cols[name] = LiteralExpression(1, Int64Type())
                else:
                    projection_cols[name] = CallExpression(
                        pydop.IFF,
                        Int64Type(),
                        [
                            CallExpression(
                                pydop.PRESENT, BooleanType(), [agg_call.inputs[0]]
                            ),
                            LiteralExpression(1, Int64Type()),
                            LiteralExpression(0, Int64Type()),
                        ],
                    )
            # If any other aggregations are present besides the supported
            # operators, skip the deletion operation and just return the
            # original aggregation.
            case _:
                return agg
    return Project(agg.input, projection_cols)


def deduce_join_uniqueness(
    join: Join, input_unique_sets: list[set[frozenset[str]]]
) -> set[frozenset[str]]:
    """
    Infers the unique column sets of a join based on its join condition and the
    uniqueness sets of its inputs.
    """
    # Reject unless doing a join on two inputs.
    if len(join.inputs) != 2:
        return set()
    lhs_unique, rhs_unique = input_unique_sets

    # If doing a semi/anti join, just use the LHS uniqueness.
    if join.join_types[0] in (JoinType.SEMI, JoinType.ANTI):
        return bubble_uniqueness(
            lhs_unique, join.columns, join.default_input_aliases[0]
        )

    # Convert the uniqueness sets from sets of column names to sets of tuples
    # in the form (is_lhs, column_name) to distinguish the lhs columns from the
    # rhs columns. Then, create the `result_pairs` set from the cartesian
    # product of all lhs/rhs uniqueness sets.
    result_pairs: set[frozenset[tuple[bool, str]]] = set()
    lhs_pairs: set[frozenset[tuple[bool, str]]] = {
        frozenset({(True, k) for k in unique_set}) for unique_set in lhs_unique
    }
    rhs_pairs: set[frozenset[tuple[bool, str]]] = {
        frozenset({(False, k) for k in unique_set}) for unique_set in rhs_unique
    }
    for lhs_set in lhs_pairs:
        for rhs_set in rhs_pairs:
            result_pairs.add(lhs_set | rhs_set)

    # Using the equi-join keys, build remappings from columns in the lhs
    # uniqueness sets to corresponding columns in the rhs uniqueness sets, and
    # vice versa.
    remappings: dict[tuple[bool, str], set[tuple[bool, str]]] = {}
    lhs_keys, rhs_keys = extract_equijoin_keys(join)
    for lhs_key, rhs_key in zip(lhs_keys, rhs_keys):
        remappings[(True, lhs_key.name)] = remappings.get((True, lhs_key.name), set())
        remappings[(True, lhs_key.name)].add((False, rhs_key.name))
        remappings[(False, rhs_key.name)] = remappings.get((False, rhs_key.name), set())
        remappings[(False, rhs_key.name)].add((True, lhs_key.name))

    # Using this mapping, update the uniqueness set by adding each variant of
    # a uniqueness set where a column from one side is replaced by a column
    # from the other side that it is inferred to e equal to based on the join
    # condition.
    new_pairs: set[frozenset[tuple[bool, str]]]
    for remapping_key in remappings:
        new_pairs = set()
        for pair_set in result_pairs:
            if remapping_key in pair_set:
                for alternate in remappings[remapping_key]:
                    new_pairs.add(
                        pair_set.difference({remapping_key}).union({alternate})
                    )
        result_pairs.update(new_pairs)

    # If any of the uniqueness sets are entirely in one side, and are a
    # strict superset of the uniqueness keys of that side, add the
    # corresponding uniqueness sets from that side (this occurs if the ioin
    # did not alter the cardinality of that side).
    new_pairs = set()
    for pair_set in result_pairs:
        if all(is_lhs for is_lhs, _ in pair_set):
            for lhs_pair_set in lhs_pairs:
                if pair_set > lhs_pair_set:
                    new_pairs.update(lhs_pairs)
                    break
        if all(not is_lhs for is_lhs, _ in pair_set):
            for rhs_pair_set in rhs_pairs:
                if pair_set > rhs_pair_set:
                    new_pairs.update(rhs_pairs)
                    break
    result_pairs.update(new_pairs)

    # Remove any uniqueness sets that are strict supersets of other uniqueness
    # sets.
    redundant_uniqueness_sets: list[frozenset[tuple[bool, str]]] = [
        set_a for set_a in result_pairs for set_b in result_pairs if set_a > set_b
    ]
    for redundant_set in redundant_uniqueness_sets:
        result_pairs.discard(redundant_set)

    # Finally, convert from the tuple form back to just uniqueness sets of
    # column names using the output column names of the join. Any uniqueness
    # sets that contain columns not present in the outut are discarded.
    result: set[frozenset[str]] = set()
    for unique_set in result_pairs:
        if len(unique_set) == 0:
            result.add(frozenset())
            continue
        # Split the uniqueness set into its columns from the lhs vs rhs, and
        # drop the boolean tuple.
        lhs_elem_names: set[str] = {k for is_lhs, k in unique_set if is_lhs}
        rhs_elem_names: set[str] = {k for is_lhs, k in unique_set if not is_lhs}
        # Propagate the columns from the lhs through the join output columns.
        # If this is possible, `new_lhs_elems` will be a singleton set whose
        # element is all of the lhs columns bubbled through. If not, it will be
        # an empty set.
        new_lhs_elems: set[frozenset[str]]
        if len(lhs_elem_names) > 0:
            new_lhs_elems = bubble_uniqueness(
                {frozenset(lhs_elem_names)}, join.columns, join.default_input_aliases[0]
            )
        else:
            new_lhs_elems = set()
        # Repeat the same idea for the rhs.
        new_rhs_elems: set[frozenset[str]]
        if len(rhs_elem_names) > 0:
            new_rhs_elems = bubble_uniqueness(
                {frozenset(rhs_elem_names)}, join.columns, join.default_input_aliases[1]
            )
        else:
            new_rhs_elems = set()
        # If both sides were able to have their columns bubbled through (or did
        # not have columns from that side to begin with), then create the new
        # output uniqueness set by combining their output columns from their
        # singleton uniqueness sets (or ignoring them if there were no columns
        # from that side to begin with)
        if len(new_lhs_elems) == len(lhs_elem_names) and len(new_rhs_elems) == len(
            rhs_elem_names
        ):
            assert len(new_lhs_elems) <= 1 and len(new_rhs_elems) <= 1
            new_set: frozenset[str] = frozenset()
            if len(new_lhs_elems) > 0:
                new_set = new_set.union(new_lhs_elems.pop())
            if len(new_rhs_elems) > 0:
                new_set = new_set.union(new_rhs_elems.pop())
            result.add(new_set)

    return result


def aggregation_uniqueness_helper(
    node: RelationalNode,
) -> tuple[RelationalNode, set[frozenset[str]]]:
    """
    Core helper function that runs the aggregation uniqueness removal,
    returning the transformed node as well as a set of all sets of column
    names that can be used to uniquely identify the rows of the returned
    node.

    Args:
        `node`: the node to be transformed.

    Returns:
        The transformed node and a set of all sets of column names that can
        be used to uniquely identify the rows of the returned node.
    """
    input_uniqueness: set[frozenset[str]]
    match node:
        # Scans are unchanged, and their uniqueness is based on the unique sets
        # of the underlying table.
        case Scan():
            return node, bubble_uniqueness(node.unique_sets, node.columns, None)
        # For filters, projections, limits and roots, the node is unchanged,
        # and the uniqueness should be propagated upward through the selected
        # columns.
        case Filter() | Project() | Limit() | RelationalRoot():
            node._input, input_uniqueness = aggregation_uniqueness_helper(node.input)
            input_uniqueness = bubble_uniqueness(input_uniqueness, node.columns, None)
            return node, input_uniqueness
        # For aggregations, the output uniqueness is the aggregation keys.
        # However, if this is a superset of any of the unique sets of the input
        # then the aggregation should be deleted since it is redundant.
        case Aggregate():
            node._input, input_uniqueness = aggregation_uniqueness_helper(node.input)
            agg_keys: frozenset[str] = frozenset(node.keys)
            output_uniqueness: set[frozenset[str]] = {agg_keys}
            for unique_set in input_uniqueness:
                if agg_keys.issuperset(unique_set):
                    node = delete_aggregation(node)
                    # If deleting the aggregation, then the uniqueness of the
                    # input is propagated through the new projection.
                    output_uniqueness = bubble_uniqueness(
                        input_uniqueness, node.columns, None
                    )
                    break
            return node, output_uniqueness
        # For joins, gather the uniqueness information from each input, then
        # infer the combined uniqueness information after joining.
        case Join():
            unique_sets: list[set[frozenset[str]]] = []
            for idx, input_node in enumerate(node.inputs):
                node.inputs[idx], input_uniqueness = aggregation_uniqueness_helper(
                    input_node
                )
                unique_sets.append(input_uniqueness)
            final_uniqueness: set[frozenset[str]] = deduce_join_uniqueness(
                node, unique_sets
            )
            return node, final_uniqueness
        # Empty singletons don't have uniqueness information.
        case EmptySingleton():
            return node, set()
        case _:
            raise NotImplementedError(
                f"Unsupported node for redundant aggregation removal: {node.__class__.__name__}"
            )


def remove_redundant_aggs(root: RelationalRoot) -> RelationalRoot:
    """
    Invokes the procedure to delete aggregations that are redundant since the
    data they are called on is already unique with regards to the aggregation
    keys.

    Args:
        `node`: the root node of the relational plan to be transformed.

    Returns:
        The transformed relational root. The transformation is also done
        in-place.
    """
    result, _ = aggregation_uniqueness_helper(root)
    assert isinstance(result, RelationalRoot)
    return result
