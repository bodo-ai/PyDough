"""
TODO
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
    def visit_join(self, join: Join) -> RelationalNode:
        join_substitution: dict[RelationalExpression, RelationalExpression] = {}
        if join.join_type == JoinType.INNER:
            lhs_keys_list, rhs_keys_list = extract_equijoin_keys(join)
            if len(lhs_keys_list) > 0 and len(rhs_keys_list) > 0:
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
                if (
                    join.cardinality == JoinCardinality.SINGULAR_ACCESS
                    and rhs_keys == rhs_refs
                ):
                    for lhs_key, rhs_key in zip(lhs_keys_list, rhs_keys_list):
                        join_substitution[rhs_key] = lhs_key
                elif (
                    join.reverse_cardinality == JoinCardinality.SINGULAR_ACCESS
                    and lhs_keys == rhs_refs
                ):
                    for lhs_key, rhs_key in zip(lhs_keys_list, rhs_keys_list):
                        join_substitution[lhs_key] = rhs_key

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

        # # Find all column references in the join condition
        # col_finder = ColumnReferenceFinder()
        # col_finder.visit(join.condition)
        # col_refs = col_finder.get_column_references()

        # substitution = {}
        # for col_ref in col_refs:
        #     if add_input_name(col_ref, join.left.schema) in join.left.schema:
        #         substitution[col_ref] = add_input_name(col_ref, join.left.schema)
        #     elif add_input_name(col_ref, join.right.schema) in join.right.schema:
        #         substitution[col_ref] = add_input_name(col_ref, join.right.schema)

        # new_condition = apply_substitution(join.condition, substitution)

        # new_join: Join = Join(
        #     left=left,
        #     right=right,
        #     condition=new_condition,
        #     join_type=join.join_type,
        #     schema=join.schema
        # )

        return super().visit_join(join)


def join_key_substitution(root: RelationalNode) -> RelationalNode:
    """
    TODO
    """
    shuttle: JoinKeySubstitutionShuttle = JoinKeySubstitutionShuttle()
    return root.accept_shuttle(shuttle)
