"""
Logic for switching references to join keys from one side of a join to the other
when certain conditions are met, thus allowing the join to be removed by the
column pruner. The conditions are:
- The join is an inner join.
- The join has equi-join keys.
- The cardinality in either direction is singular-access.
- The only columns used from one side of the join (the one being referenced in
  a singular-access manner) are the join keys (or a subset thereof).
"""

from pydough.relational import (
    ColumnReference,
    ColumnReferenceFinder,
    Join,
    JoinCardinality,
    JoinType,
    RelationalExpression,
    RelationalNode,
    RelationalShuttle,
)
from pydough.relational.rel_util import (
    apply_substitution,
    extract_equijoin_keys,
)


class JoinKeySubstitutionShuttle(RelationalShuttle):
    """
    The relational shuttle that performs join key substitution optimization.
    """

    def visit_join(self, join: Join) -> RelationalNode:
        # Build up a mapping of join key substitutions mapping input columns
        # from one input to another when the optimization case is detected:
        # requires an inner join with equi-join keys.
        join_substitution: dict[RelationalExpression, RelationalExpression] = {}
        if join.join_type == JoinType.INNER:
            lhs_keys_list, rhs_keys_list = extract_equijoin_keys(join)
            if len(lhs_keys_list) > 0 and len(rhs_keys_list) > 0:
                # Identify which columns are used by the join columns that come
                # from the left and right inputs.
                lhs_keys: set[ColumnReference] = set(lhs_keys_list)
                rhs_keys: set[ColumnReference] = set(rhs_keys_list)
                col_finder = ColumnReferenceFinder()
                for value in join.columns.values():
                    value.accept(col_finder)
                col_refs: set[ColumnReference] = col_finder.get_column_references()
                lhs_refs = {
                    ref
                    for ref in col_refs
                    if ref.input_name == join.default_input_aliases[0]
                }
                rhs_refs = col_refs - lhs_refs
                # If each row on the left side (LHS) matches exactly one row on the right side (RHS)
                # (i.e., singular access)
                # and the query only references columns from the RHS that are join keys,
                # then we can substitute the RHS join keys with the corresponding LHS join keys.
                # This allows the join to potentially be removed later since it adds no new data.
                if (
                    join.cardinality == JoinCardinality.SINGULAR_ACCESS
                    and rhs_refs <= rhs_keys
                ):
                    for lhs_key, rhs_key in zip(lhs_keys_list, rhs_keys_list):
                        join_substitution[rhs_key] = lhs_key

                # If the right side is singular access, and all the columns used
                # from the left side are just the join keys, then we can
                # substitute the left join keys with the right join keys.
                elif (
                    join.reverse_cardinality == JoinCardinality.SINGULAR_ACCESS
                    and lhs_refs <= lhs_keys
                ):
                    for lhs_key, rhs_key in zip(lhs_keys_list, rhs_keys_list):
                        join_substitution[lhs_key] = rhs_key

        # If any substitutions were identified, create a new Join node
        # with the substitutions applied to its columns.
        if len(join_substitution) > 0:
            join = Join(
                join.inputs,
                join.condition,
                join.join_type,
                {
                    name: apply_substitution(expr, join_substitution, {})
                    for name, expr in join.columns.items()
                },
                join.cardinality,
                join.reverse_cardinality,
                join.correl_name,
            )

        # Recursively visit the inputs to the join to transform them as well.
        return super().visit_join(join)


def join_key_substitution(root: RelationalNode) -> RelationalNode:
    """
    The main entry point for join key substitution optimization.

    Args:
        `root`: The root of the relational tree being optimized.

    Returns:
        The optimized relational tree.
    """
    shuttle: JoinKeySubstitutionShuttle = JoinKeySubstitutionShuttle()
    return root.accept_shuttle(shuttle)
