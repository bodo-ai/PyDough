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
        # TODO: ADD COMMENTS
        match agg_call.op:
            case (
                pydop.SUM
                | pydop.MIN
                | pydop.MAX
                | pydop.AVG
                | pydop.ANYTHING
                | pydop.MEDIAN
            ):
                projection_cols[name] = agg_call.inputs[0]
            case pydop.COUNT:
                if len(agg_call.inputs) == 0:
                    projection_cols[name] = LiteralExpression(0, Int64Type())
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
            case _:
                return agg
    return Project(agg.input, projection_cols)


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
    # TODO: ADD COMMENTS
    match node:
        case Scan():
            return node, bubble_uniqueness(node.unique_sets, node.columns)
        case Filter() | Project() | Limit() | RelationalRoot():
            node._input, input_uniqueness = aggregation_uniqueness_helper(node.input)
            return node, bubble_uniqueness(input_uniqueness, node.columns)
        case Aggregate():
            node._input, input_uniqueness = aggregation_uniqueness_helper(node.input)
            agg_keys: frozenset[str] = frozenset(node.keys)
            for unique_set in input_uniqueness:
                if agg_keys.issuperset(unique_set):
                    node = delete_aggregation(node)
                    break
            return node, {agg_keys}
        case Join():
            for idx, input_node in enumerate(node.inputs):
                node.inputs[idx], _ = aggregation_uniqueness_helper(input_node)
            return node, set()
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
