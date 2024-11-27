"""
TODO: add file-level docstring
"""

__all__ = ["convert_ast_to_relational"]


from collections.abc import MutableMapping
from dataclasses import dataclass

import pydough.pydough_ast.pydough_operators as pydop
from pydough.metadata import (
    CartesianProductMetadata,
    SimpleJoinMetadata,
    SimpleTableMetadata,
)
from pydough.pydough_ast import (
    Calc,
    CollationExpression,
    CollectionAccess,
    CompoundSubCollection,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Reference,
    SubCollection,
    TableCollection,
)
from pydough.relational.relational_expressions import (
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    LiteralExpression,
    RelationalExpression,
)
from pydough.relational.relational_nodes import (
    ColumnPruner,
    Join,
    JoinType,
    Project,
    Relational,
    RelationalRoot,
    Scan,
)
from pydough.types import BooleanType

from .hybrid_tree import (
    HybridCalc,
    HybridCollectionAccess,
    HybridColumnExpr,
    HybridExpr,
    HybridLiteralExpr,
    HybridOperation,
    HybridRefExpr,
    HybridTree,
    make_hybrid_tree,
)


@dataclass
class TranslationOutput:
    relation: Relational
    expressions: dict[HybridExpr, ColumnReference]


class RelTranslation:
    def translate_expression(
        self, expr: HybridExpr, context: TranslationOutput
    ) -> RelationalExpression:
        """
        TODO: add function docstring
        """
        match expr:
            case HybridLiteralExpr():
                return LiteralExpression(expr.literal.value, expr.typ)
            case HybridRefExpr():
                return context.expressions[expr]
            case _:
                raise NotImplementedError(expr.__class__.__name__)

    def build_simple_table_scan(
        self, node: HybridCollectionAccess, table_path: str
    ) -> TranslationOutput:
        """
        TODO: add function docstring
        """
        out_columns: dict[HybridExpr, ColumnReference] = {}
        scan_columns: dict[str, RelationalExpression] = {}
        for expr_name in node.terms:
            hybrid_expr = node.terms[expr_name]
            assert isinstance(hybrid_expr, HybridColumnExpr)
            hybrid_ref: HybridRefExpr = HybridRefExpr(expr_name, hybrid_expr.typ)
            scan_ref: ColumnReference = ColumnReference(
                hybrid_expr.column.column_property.column_name, hybrid_expr.typ
            )
            out_ref: ColumnReference = ColumnReference(expr_name, hybrid_expr.typ)
            scan_columns[expr_name] = scan_ref
            out_columns[hybrid_ref] = out_ref
        answer = Scan(table_path, scan_columns)
        return TranslationOutput(answer, out_columns)

    def translate_table_collection(
        self, node: HybridCollectionAccess
    ) -> TranslationOutput:
        """
        TODO: add function docstring
        """
        collection_access: CollectionAccess = node.collection
        assert isinstance(collection_access, TableCollection)
        assert isinstance(
            collection_access.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {collection_access.collection.__class__.__name__}"
        return self.build_simple_table_scan(
            node, collection_access.collection.table_path
        )

    def translate_sub_collection(
        self,
        node: HybridCollectionAccess,
        parent: HybridTree,
        context: TranslationOutput,
    ) -> TranslationOutput:
        """
        TODO: add function docstring
        """

        # First, build the table scan for the collection being stepped into.
        collection_access: CollectionAccess = node.collection
        assert isinstance(collection_access, SubCollection)
        assert isinstance(
            collection_access.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {collection_access.collection.__class__.__name__}"
        rhs_output: TranslationOutput = self.build_simple_table_scan(
            node, collection_access.collection.table_path
        )

        # Create the join node so we know what aliases it uses, but leave
        # the condition as always-True and the output columns empty for now.
        out_columns: dict[HybridExpr, ColumnReference] = {}
        join_columns: dict[str, RelationalExpression] = {}
        out_rel: Join = Join(
            [context.relation, rhs_output.relation],
            [LiteralExpression(True, BooleanType())],
            [JoinType.INNER],
            join_columns,
        )
        input_aliases: list[str | None] = out_rel.default_input_aliases

        if isinstance(collection_access.subcollection_property, SimpleJoinMetadata):
            # If the subcollection is a simple join property, extract the keys
            # and build the corresponding (lhs_key == rhs_key) conditions
            cond_terms: list[RelationalExpression] = []
            for lhs_name in collection_access.subcollection_property.keys:
                lhs_expr: HybridExpr = parent.pipeline[-1].terms[lhs_name]
                lhs_expr = HybridRefExpr(lhs_name, lhs_expr.typ)
                lhs_key: ColumnReference = context.expressions[lhs_expr].with_input(
                    input_aliases[0]
                )
                for rhs_name in collection_access.subcollection_property.keys[lhs_name]:
                    rhs_expr: HybridExpr = node.terms[rhs_name]
                    rhs_expr = HybridRefExpr(rhs_name, rhs_expr.typ)
                    rhs_key: ColumnReference = rhs_output.expressions[
                        rhs_expr
                    ].with_input(input_aliases[1])
                    cond: RelationalExpression = CallExpression(
                        pydop.EQU, BooleanType(), [lhs_key, rhs_key]
                    )
                    cond_terms.append(cond)
            # Build the condition as the conjunction of `cond_terms`
            join_cond: RelationalExpression = cond_terms[0]
            for i in range(1, len(cond_terms)):
                join_cond = CallExpression(
                    pydop.BAN, BooleanType(), [join_cond, cond_terms[i]]
                )
            out_rel._conditions[0] = join_cond
        elif not isinstance(
            collection_access.subcollection_property, CartesianProductMetadata
        ):
            raise NotImplementedError()

        # Redecorate all of the predecessor terms with the LHS alias, and add
        # to the output columns of the JOIN node.
        for expr in context.expressions:
            new_ancestor_reference: ColumnReference = context.expressions[
                expr
            ].with_input(input_aliases[0])
            context.expressions[expr] = new_ancestor_reference
            join_columns[new_ancestor_reference.name] = new_ancestor_reference
        for expr in rhs_output.expressions:
            old_reference = rhs_output.expressions[expr]
            old_name: str = old_reference.name
            new_name: str = old_name
            idx: int = 1
            while new_name in join_columns:
                new_name = f"{old_name}_{idx}"
                idx += 1
            new_reference: ColumnReference = ColumnReference(
                new_name, old_reference.data_type
            )
            join_columns[new_name] = old_reference.with_input(input_aliases[1])
            out_columns[expr] = new_reference
        return TranslationOutput(out_rel, out_columns)

    def translate_calc(
        self,
        node: HybridCalc,
        context: TranslationOutput,
    ) -> TranslationOutput:
        """
        TODO: add function docstring
        """
        proj_columns: dict[str, RelationalExpression] = {}
        out_columns: dict[HybridExpr, ColumnReference] = {}
        out_rel: Project = Project(context.relation, proj_columns)
        in_columns: MutableMapping[str, RelationalExpression] = context.relation.columns
        in_expressions: dict[HybridExpr, ColumnReference] = context.expressions
        for name in in_columns:
            proj_columns[name] = in_columns[name]
        for expr in in_expressions:
            out_columns[expr] = in_expressions[expr]
        for name in node.terms:
            hybrid_expr: HybridExpr = node.terms[name]
            ref_expr: HybridRefExpr = HybridRefExpr(name, hybrid_expr.typ)
            rel_expr: RelationalExpression = self.translate_expression(
                hybrid_expr, context
            )
            proj_columns[name] = rel_expr
            out_columns[ref_expr] = ColumnReference(name, rel_expr.data_type)
        return TranslationOutput(out_rel, out_columns)

    def rel_translation(
        self,
        hybrid: HybridTree,
        pipeline_idx: int,
    ) -> TranslationOutput:
        """
        TODO: add function docstring
        """
        assert pipeline_idx < len(
            hybrid.pipeline
        ), f"Pipeline index {pipeline_idx} is too big for hybrid tree:\n{hybrid}"

        # TODO: deal with ancestors of a table collection
        operation: HybridOperation = hybrid.pipeline[pipeline_idx]
        if isinstance(operation, HybridCollectionAccess) and isinstance(
            operation.collection, TableCollection
        ):
            return self.translate_table_collection(operation)

        context: TranslationOutput
        parent: HybridTree | None = hybrid.parent
        if pipeline_idx == 0:
            assert parent is not None
            context = self.rel_translation(parent, len(parent.pipeline) - 1)
        else:
            context = self.rel_translation(hybrid, pipeline_idx - 1)

        match operation:
            case HybridCollectionAccess():
                if isinstance(operation.collection, SubCollection) and not isinstance(
                    operation.collection, CompoundSubCollection
                ):
                    assert parent is not None
                    return self.translate_sub_collection(operation, parent, context)
                else:
                    raise NotImplementedError(
                        f"TODO: support relational conversion on {operation.__class__.__name__}"
                    )
            case HybridCalc():
                return self.translate_calc(operation, context)
            case _:
                raise NotImplementedError(
                    f"TODO: support relational conversion on {operation.__class__.__name__}"
                )

    @staticmethod
    def preprocess_root(
        node: PyDoughCollectionAST,
    ) -> tuple[PyDoughCollectionAST, list[CollationExpression]]:
        """
        Transforms the final PyDough collection by appending it with an extra CALC
        containing all of the columns that are outputted or used for ordering.
        """
        # Fetch all of the expressions that should be kept in the final output
        original_calc_terms: set[str] = node.calc_terms
        children: list[PyDoughCollectionAST] = []
        final_terms: list[tuple[str, PyDoughExpressionAST]] = []
        all_names: set[str] = set()
        for name in original_calc_terms:
            final_terms.append((name, Reference(node, name)))
            all_names.add(name)
        final_terms.sort(key=lambda term: node.get_expression_position(term[0]))
        dummy_counter = 0
        final_calc: Calc = Calc(node, children)

        # Add all of the expressions that are used as ordering keys,
        # transforming any non-references into references.
        ordering: list[CollationExpression] = []
        if node.ordering is not None:
            for expr in node.ordering:
                if isinstance(expr.expr, Reference):
                    ordering.append(expr)
                else:
                    dummy_name: str
                    while True:
                        dummy_name = f"_order_expr_{dummy_counter}"
                        dummy_counter += 1
                        if dummy_name not in all_names:
                            break
                    final_terms.append((dummy_name, expr.expr))
                    all_names.add(dummy_name)
                    ordering.append(
                        CollationExpression(
                            Reference(final_calc, dummy_name), expr.asc, expr.na_last
                        )
                    )

        return final_calc.with_terms(final_terms), ordering


def convert_ast_to_relational(node: PyDoughCollectionAST) -> RelationalRoot:
    """
    TODO: add function docstring
    """
    # Pre-process the AST node so the final CALC term includes any ordering
    # keys.
    translator: RelTranslation = RelTranslation()
    final_terms: set[str] = node.calc_terms
    node, collation = translator.preprocess_root(node)

    # Convert the AST node to the hybrid form, then invoke the relational
    # conversion procedure. The first element in the returned list is the
    # final rel node.
    hybrid: HybridTree = make_hybrid_tree(node)
    renamings: dict[str, str] = hybrid.pipeline[-1].renamings
    #######################################################################
    ###              FOR DEBUGGING: UNCOMMENT THIS SECTION              ###
    #######################################################################
    # base_hybrid: HybridTree = hybrid
    # while base_hybrid.parent is not None:
    #     base_hybrid = base_hybrid.parent
    # print(base_hybrid)
    output: TranslationOutput = translator.rel_translation(
        hybrid, len(hybrid.pipeline) - 1
    )
    ordered_columns: list[tuple[str, RelationalExpression]] = []
    orderings: list[ExpressionSortInfo] | None = None

    # Extract the relevant expressions for the final columns and ordering keys
    # so that the root node can be built from them.
    hybrid_expr: HybridExpr
    rel_expr: RelationalExpression
    name: str
    original_name: str
    positions: dict[str, int] = {}
    for original_name in final_terms:
        name = renamings.get(original_name, original_name)
        hybrid_expr = hybrid.pipeline[-1].terms[name]
        rel_expr = output.expressions[hybrid_expr]
        ordered_columns.append((name, rel_expr))
        positions[name] = node.get_expression_position(original_name)
    ordered_columns.sort(key=lambda col: positions[col[0]])
    if collation is not None:
        orderings = []
        for col_expr in collation:
            raw_expr = col_expr.expr
            assert isinstance(raw_expr, Reference)
            original_name = raw_expr.term_name
            name = renamings.get(original_name, original_name)
            hybrid_expr = HybridRefExpr(name, raw_expr.pydough_type)
            relational_expr = output.expressions[hybrid_expr]
            if not isinstance(relational_expr, ColumnReference):
                raise NotImplementedError(
                    "TODO: support root ordering on expressions besides column references"
                )
            collation_expr: ExpressionSortInfo = ExpressionSortInfo(
                relational_expr, col_expr.asc, not col_expr.na_last
            )
            orderings.append(collation_expr)
    unpruned_result: RelationalRoot = RelationalRoot(
        output.relation, ordered_columns, orderings
    )
    return ColumnPruner().prune_unused_columns(unpruned_result)
