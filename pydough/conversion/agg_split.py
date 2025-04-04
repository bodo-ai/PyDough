"""
Logic used to partially transpose aggregates beneath joins when splittable into
a partial aggregation.
"""

__all__ = ["split_partial_aggregates"]


import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    Join,
    RelationalNode,
)
from pydough.relational.rel_util import (
    transpose_expression,
)


def split_partial_aggregates(node: RelationalNode) -> RelationalNode:
    """
    TODO
    """
    if isinstance(node, Aggregate) and isinstance(node.input, Join):
        # 1. Verify all of the aggfuncs are from the splittable list.
        #    If AVG is in the list, but there is at least 1 non-AVG,
        #    convert AVG into SUM/COUNT and recursively repeat.
        # 2. Verify join is purely an equi-join.
        # 3. Verify all of the aggregation inputs come from the same side of
        # the join.
        # 4. Verify that side of the join is an INNER join.
        # 5. If any of 1-4 are false, move on.
        # 6. Replace each aggfunc with its partial top version
        # 7. Insert into the specified side of the join an aggregate with the
        # same agg keys plus all of the join keys as additional agg keys (if
        # not already in the agg keys).
        join: Join = node.input
        if True:
            top_aggs: dict[str, CallExpression] = {}
            input_aggs: dict[str, CallExpression] = {}
            for name, agg in node.aggregations.items():
                top_aggfunc: pydop.PyDoughExpressionOperator = agg.op
                bottom_aggfunc: pydop.PyDoughExpressionOperator = agg.op
                top_aggs[name] = CallExpression(
                    top_aggfunc, agg.data_type, [ColumnReference(name, agg.data_type)]
                )
                input_aggs[name] = CallExpression(
                    bottom_aggfunc,
                    agg.data_type,
                    [transpose_expression(arg, join.columns) for arg in agg.inputs],
                )

    # Recursively invoke the procedure on all inputs to the node.
    return node.copy(inputs=[split_partial_aggregates(input) for input in node.inputs])
