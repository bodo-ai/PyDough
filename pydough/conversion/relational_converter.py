"""
TODO: add file-level docstring
"""

__all__ = ["convert_ast_to_relational"]


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
    ConnectionType,
    HybridBackRefExpr,
    HybridCalc,
    HybridChildRefExpr,
    HybridCollectionAccess,
    HybridColumnExpr,
    HybridConnection,
    HybridExpr,
    HybridFunctionExpr,
    HybridLiteralExpr,
    HybridOperation,
    HybridRefExpr,
    HybridRoot,
    HybridTree,
    make_hybrid_tree,
)


@dataclass
class TranslationOutput:
    """
    The output payload for the conversion of a HybridTree prefix to
    a Relational structure. Contains the Relational node tree in question,
    as well as a mapping that can be used to identify what column to use to
    access any HybridExpr's equivalent expression in the Relational node.
    """

    relation: Relational
    expressions: dict[HybridExpr, ColumnReference]


class RelTranslation:
    def __init__(self):
        # An index used for creating fake column names
        self.dummy_idx = 1

    def translate_expression(
        self, expr: HybridExpr, context: TranslationOutput | None
    ) -> RelationalExpression:
        """
        Converts a HybridExpr to a RelationalExpression node based on the
        current context. NOTE: currently only supported for literals, columns,
        and column references.

        Args:
            `expr`: the HybridExpr node to be converted.
            `context`: the datastructure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts. Can be omitted in certain contexts, such as
            when deriving a table scan or literal.

        Returns:
            The converted relational expression.
        """
        match expr:
            case HybridColumnExpr():
                return ColumnReference(
                    expr.column.column_property.column_name, expr.typ
                )
            case HybridLiteralExpr():
                return LiteralExpression(expr.literal.value, expr.typ)
            case HybridRefExpr() | HybridChildRefExpr() | HybridBackRefExpr():
                assert context is not None
                return context.expressions[expr]
            case HybridFunctionExpr() if not expr.operator.is_aggregation:
                assert context is not None
                inputs: list[RelationalExpression] = [
                    self.translate_expression(arg, context) for arg in expr.args
                ]
                return CallExpression(expr.operator, expr.typ, inputs)
            case _:
                raise NotImplementedError(expr.__class__.__name__)

    def build_simple_table_scan(
        self, node: HybridCollectionAccess
    ) -> TranslationOutput:
        """
        Converts an access of a collection into a table scan.

        Args:
            `node`: the node corresponding to accessing a collection
            (could be a standalone table collection or subcollection access).

        Returns:
            The TranslationOutput payload containing the table scan as well
            as the expression mappings so future references know how to
            access the table columns.
        """
        out_columns: dict[HybridExpr, ColumnReference] = {}
        scan_columns: dict[str, RelationalExpression] = {}
        for expr_name in node.terms:
            hybrid_expr: HybridExpr = node.terms[expr_name]
            scan_ref: RelationalExpression = self.translate_expression(
                hybrid_expr, None
            )
            assert isinstance(scan_ref, ColumnReference)
            scan_columns[expr_name] = scan_ref
            hybrid_ref: HybridRefExpr = HybridRefExpr(expr_name, hybrid_expr.typ)
            out_ref: ColumnReference = ColumnReference(expr_name, hybrid_expr.typ)
            out_columns[hybrid_ref] = out_ref
        assert isinstance(
            node.collection.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {node.collection.collection.__class__.__name__}"
        answer = Scan(node.collection.collection.table_path, scan_columns)
        return TranslationOutput(answer, out_columns)

    def translate_sub_collection(
        self,
        node: HybridCollectionAccess,
        parent: HybridTree,
        context: TranslationOutput,
    ) -> TranslationOutput:
        """
        Converts a subcollection access into a join from the parent onto
        a scan of the child.

        Args:
            `node`: the node corresponding to the subcollection access.
            `parent`: the hybrid tree of the previous layer that the access
            steps down from.
            `context`: the datastructure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts. Can be omitted in certain contexts, such as
            when deriving a table scan or literal.

        Returns:
            The TranslationOutput payload containing an INNER join of the
            relational node for the parent and the table scan of the child.
        """

        # First, build the table scan for the collection being stepped into.
        collection_access: CollectionAccess = node.collection
        assert isinstance(collection_access, SubCollection)
        assert isinstance(
            collection_access.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {collection_access.collection.__class__.__name__}"
        rhs_output: TranslationOutput = self.build_simple_table_scan(node)

        join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        if isinstance(collection_access.subcollection_property, SimpleJoinMetadata):
            # If the subcollection is a simple join property, extract the keys
            # and build the corresponding (lhs_key == rhs_key) conditions
            for lhs_name in collection_access.subcollection_property.keys:
                lhs_key: HybridExpr = (
                    parent.pipeline[-1].terms[lhs_name].make_into_ref(lhs_name)
                )
                for rhs_name in collection_access.subcollection_property.keys[lhs_name]:
                    rhs_key: HybridExpr = node.terms[rhs_name].make_into_ref(rhs_name)
                    join_keys.append((lhs_key, rhs_key))
        elif not isinstance(
            collection_access.subcollection_property, CartesianProductMetadata
        ):
            raise NotImplementedError(
                f"Unsupported subcollection property type used for accessing a subcollection: {collection_access.subcollection_property.__class__.__name__}"
            )

        return self.join_outputs(
            parent.pipeline[-1],
            context,
            node,
            rhs_output,
            JoinType.INNER,
            join_keys,
            None,
        )

    def translate_calc(
        self,
        node: HybridCalc,
        context: TranslationOutput,
    ) -> TranslationOutput:
        """
        Converts a calc into a project on top of its child to derive additional
        terms.

        Args:
            `node`: the node corresponding to the calc being derived.
            `parent`: the hybrid tree of the previous layer that the access
            steps down from.
            `context`: the datastructure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts. Can be omitted in certain contexts, such as
            when deriving a table scan or literal.

        Returns:
            The TranslationOutput payload containing a PROJECT on top of
            the relational node for the parent to derive any additional terms.
        """
        proj_columns: dict[str, RelationalExpression] = {}
        out_columns: dict[HybridExpr, ColumnReference] = {}
        # Populate every expression into the project's columns by translating
        # it relative to the input context.
        for name in node.terms:
            hybrid_expr: HybridExpr = node.terms[name]
            ref_expr: HybridRefExpr = HybridRefExpr(name, hybrid_expr.typ)
            rel_expr: RelationalExpression = self.translate_expression(
                hybrid_expr, context
            )
            proj_columns[name] = rel_expr
            out_columns[ref_expr] = ColumnReference(name, rel_expr.data_type)
        out_rel: Project = Project(context.relation, proj_columns)
        return TranslationOutput(out_rel, out_columns)

    def join_outputs(
        self,
        lhs_hybrid: HybridOperation,
        lhs_result: TranslationOutput,
        rhs_hybrid: HybridOperation,
        rhs_result: TranslationOutput,
        join_type: JoinType,
        join_keys: list[tuple[HybridExpr, HybridExpr]],
        child_idx: int | None,
    ) -> TranslationOutput:
        """
        TODO
        """
        # Create the join node so we know what aliases it uses, but leave
        # the condition as always-True and the output columns empty for now.
        # The condition & output columns will be filled in later.
        out_columns: dict[HybridExpr, ColumnReference] = {}
        join_columns: dict[str, RelationalExpression] = {}
        out_rel: Join = Join(
            [lhs_result.relation, rhs_result.relation],
            [LiteralExpression(True, BooleanType())],
            [join_type],
            join_columns,
        )
        input_aliases: list[str | None] = out_rel.default_input_aliases

        # Build the corresponding (lhs_key == rhs_key) conditions
        cond_terms: list[RelationalExpression] = []
        for lhs_key, rhs_key in join_keys:
            lhs_key_column: ColumnReference = lhs_result.expressions[
                lhs_key
            ].with_input(input_aliases[0])
            rhs_key_column: ColumnReference = rhs_result.expressions[
                rhs_key
            ].with_input(input_aliases[1])
            cond: RelationalExpression = CallExpression(
                pydop.EQU, BooleanType(), [lhs_key_column, rhs_key_column]
            )
            cond_terms.append(cond)
        out_rel.conditions[0] = RelationalExpression.form_conjunction(cond_terms)

        # Propagate all of the references from the left hand side. If the join
        # is being done to step down from a parent into a child then promote
        # the back levels of the reference by 1. If the join is being done to
        # pull elements from the child context into the current context, then
        # maintain them as-is.
        for expr in lhs_result.expressions:
            existing_ref: ColumnReference = lhs_result.expressions[expr]
            join_columns[existing_ref.name] = existing_ref.with_input(input_aliases[0])
            if child_idx is None:
                out_columns[expr.shift_back(1)] = existing_ref
            else:
                out_columns[expr] = existing_ref

        # Add all of the new references from the right hand side (in
        # alphabetical order).
        expr_refs: list[tuple[HybridExpr, ColumnReference]] = list(
            rhs_result.expressions.items()
        )
        expr_refs.sort(key=lambda pair: pair[1].name)
        for expr, old_reference in expr_refs:
            # If the join is being done to pull elements from the child context
            # into the current context, then promote the references to child
            # references.
            if child_idx is not None:
                if not isinstance(expr, HybridRefExpr):
                    continue
                expr = HybridChildRefExpr(expr.name, child_idx, expr.typ)
            # Names from the LHS are maintained as-is, so if there is a
            # an overlapping name in the RHS, a new name must be found.
            old_name: str = old_reference.name
            new_name: str = old_name
            while new_name in join_columns:
                new_name = f"{old_name}_{self.dummy_idx}"
                self.dummy_idx += 1
            new_reference: ColumnReference = ColumnReference(
                new_name, old_reference.data_type
            )
            join_columns[new_name] = old_reference.with_input(input_aliases[1])
            out_columns[expr] = new_reference
        return TranslationOutput(out_rel, out_columns)

    def handle_child(
        self,
        result: TranslationOutput,
        operation: HybridOperation,
        child: HybridConnection,
        child_idx: int,
    ) -> TranslationOutput:
        """
        TODO
        """
        child_output, join_keys = self.translate_child(
            result, child, child.subtree, len(child.subtree.pipeline) - 1
        )
        if child.connection_type == ConnectionType.SINGULAR:
            return self.join_outputs(
                operation,
                result,
                child.subtree.pipeline[-1],
                child_output,
                JoinType.LEFT,
                join_keys,
                child_idx,
            )
        else:
            raise NotImplementedError(
                f"TODO: support connection type {child.connection_type}"
            )

    def handle_children(
        self, result: TranslationOutput, hybrid: HybridTree, pipeline_idx: int
    ) -> TranslationOutput:
        """
        TODO
        """
        for child_idx, child in enumerate(hybrid.children):
            if child.required_steps == pipeline_idx:
                result = self.handle_child(
                    result, hybrid.pipeline[pipeline_idx], child, child_idx
                )
        return result

    def translate_child_sub_collection(
        self, connection: HybridConnection, node: HybridCollectionAccess
    ) -> tuple[TranslationOutput, list[tuple[HybridExpr, HybridExpr]]]:
        """
        TODO
        """
        # First, build the table scan for the collection being stepped into.
        collection_access: CollectionAccess = node.collection
        assert isinstance(collection_access, SubCollection)
        assert isinstance(
            collection_access.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {collection_access.collection.__class__.__name__}"
        table_scan: TranslationOutput = self.build_simple_table_scan(node)

        join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        if isinstance(collection_access.subcollection_property, SimpleJoinMetadata):
            # If the subcollection is a simple join property, extract the keys
            # and build the corresponding (lhs_key == rhs_key) conditions
            for lhs_name in collection_access.subcollection_property.keys:
                lhs_key: HybridExpr = (
                    connection.parent.pipeline[-1]
                    .terms[lhs_name]
                    .make_into_ref(lhs_name)
                )
                for rhs_name in collection_access.subcollection_property.keys[lhs_name]:
                    rhs_key: HybridExpr = node.terms[rhs_name].make_into_ref(rhs_name)
                    join_keys.append((lhs_key, rhs_key))
        elif not isinstance(
            collection_access.subcollection_property, CartesianProductMetadata
        ):
            raise NotImplementedError(
                f"Unsupported subcollection property type used for accessing a subcollection: {collection_access.subcollection_property.__class__.__name__}"
            )
        return table_scan, join_keys

    def translate_child(
        self,
        parent_result: TranslationOutput,
        connection: HybridConnection,
        hybrid: HybridTree,
        pipeline_idx: int,
    ) -> tuple[TranslationOutput, list[tuple[HybridExpr, HybridExpr]]]:
        """
        TODO:
        """
        assert pipeline_idx < len(
            hybrid.pipeline
        ), f"Pipeline index {pipeline_idx} is too big for hybrid tree:\n{hybrid}"

        # Identify the operation that will be computed at this stage.
        operation: HybridOperation = hybrid.pipeline[pipeline_idx]

        # Base case: if at the start, process the beginning of the child
        # subtree relative to the parent. Currently only supports cases where
        # the base is directly accessing a table collection or is a single-step
        # subcollection access.
        result: TranslationOutput
        result_join_keys: list[tuple[HybridExpr, HybridExpr]]
        if pipeline_idx == 0 and hybrid.parent is None:
            if isinstance(operation, HybridCollectionAccess):
                if isinstance(operation.collection, TableCollection):
                    result = self.build_simple_table_scan(operation)
                    result_join_keys = []
                elif isinstance(operation.collection, SubCollection) and not isinstance(
                    operation.collection, CompoundSubCollection
                ):
                    result, result_join_keys = self.translate_child_sub_collection(
                        connection, operation
                    )
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError
            result = self.handle_children(result, hybrid, pipeline_idx)
            return result, result_join_keys

        # First, recursively fetch the TranslationOutput of the preceding
        # operation on the current level of the hybrid tree, or the last
        # operation from the preceding level if we are at the start of the
        # current level.
        context: TranslationOutput
        if pipeline_idx > 0:
            context, result_join_keys = self.translate_child(
                parent_result, connection, hybrid, pipeline_idx - 1
            )
        elif hybrid.parent is not None:
            context, result_join_keys = self.translate_child(
                parent_result,
                connection,
                hybrid.parent,
                len(hybrid.parent.pipeline) - 1,
            )
        else:
            raise ValueError("Malformed case")

        # Then, dispatch onto the logic to transform from the context into the
        # new translation output.
        match operation:
            case HybridCollectionAccess():
                if isinstance(operation.collection, SubCollection) and not isinstance(
                    operation.collection, CompoundSubCollection
                ):
                    assert hybrid.parent is not None
                    result = self.translate_sub_collection(
                        operation, hybrid.parent, context
                    )
                    result_join_keys = [
                        (lhs_key, rhs_key.shift_back(1))
                        for lhs_key, rhs_key in result_join_keys
                    ]
                else:
                    raise NotImplementedError(
                        f"TODO: support relational conversion on {operation.__class__.__name__}"
                    )
            case HybridCalc():
                result = self.translate_calc(operation, context)
            case _:
                raise NotImplementedError(
                    f"TODO: support relational conversion on {operation.__class__.__name__}"
                )
        result = self.handle_children(result, hybrid, pipeline_idx)
        return result, result_join_keys

    def rel_translation(
        self,
        hybrid: HybridTree,
        pipeline_idx: int,
    ) -> TranslationOutput:
        """
        The recursive procedure for converting a prefix of the hybrid tree
        into a TranslationOutput payload.

        Args:
            `hybrid`: the current level of the hybrid tree to be derived,
            including all levels before it.
            `pipeline_idx`: the index of the operation in the pipeline of the
            current level that is to be derived, as well as all operations
            preceding it in the pipeline.

        Returns:
            The TranslationOutput payload corresponding to the relational
            node to derive the prefix of the hybrid tree up to the level of
            `hybrid` from all pipeline operators up to and including the
            value of `pipeline_idx`.
        """
        assert pipeline_idx < len(
            hybrid.pipeline
        ), f"Pipeline index {pipeline_idx} is too big for hybrid tree:\n{hybrid}"

        # Identify the operation that will be computed at this stage.
        operation: HybridOperation = hybrid.pipeline[pipeline_idx]

        # Identify the preceding hybrid tree operation on the current level,
        # or the last operation from the previous level if at the start, or
        # none if we have arrived at the true start.
        preceding_tree: HybridTree
        preceding_idx: int
        if pipeline_idx > 0:
            preceding_tree = hybrid
            preceding_idx = pipeline_idx - 1
        elif hybrid.parent is not None:
            preceding_tree = hybrid.parent
            preceding_idx = len(hybrid.parent.pipeline) - 1
        else:
            raise NotImplementedError()

        # Base case: deal with operations that do not contend with a
        # predecessor. TODO: deal with global CALC operations.
        result: TranslationOutput
        if isinstance(preceding_tree.pipeline[preceding_idx], HybridRoot):
            if isinstance(operation, HybridCollectionAccess) and isinstance(
                operation.collection, TableCollection
            ):
                result = self.build_simple_table_scan(operation)
                return self.handle_children(result, hybrid, pipeline_idx)
            else:
                raise NotImplementedError()

        # First, recursively fetch the TranslationOutput of the preceding
        # operation on the current level of the hybrid tree, or the last
        # operation from the preceding level if we are at the start of the
        # current lavel.
        context: TranslationOutput = self.rel_translation(preceding_tree, preceding_idx)

        # Then, dispatch onto the logic to transform from the context into the
        # new translation output.
        match operation:
            case HybridCollectionAccess():
                if isinstance(operation.collection, SubCollection) and not isinstance(
                    operation.collection, CompoundSubCollection
                ):
                    assert hybrid.parent is not None
                    result = self.translate_sub_collection(
                        operation, hybrid.parent, context
                    )
                else:
                    raise NotImplementedError(
                        f"TODO: support relational conversion on {operation.__class__.__name__}"
                    )
            case HybridCalc():
                result = self.translate_calc(operation, context)
            case _:
                raise NotImplementedError(
                    f"TODO: support relational conversion on {operation.__class__.__name__}"
                )
        return self.handle_children(result, hybrid, pipeline_idx)

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
    Main API for converting from the collection AST form into relational
    nodes.

    Args:
        `node`: the PyDough AST collection node to be translated.

    Returns:
        The RelationalRoot for the entire PyDough calculation that the
        collection node corresponds to. Ensures that the calc terms of
        `node` are included in the root in the correct order, and if it
        has an ordering then the relational root stores that information.
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
    for original_name in final_terms:
        name = renamings.get(original_name, original_name)
        hybrid_expr = hybrid.pipeline[-1].terms[name]
        rel_expr = output.expressions[hybrid_expr]
        ordered_columns.append((original_name, rel_expr))
    ordered_columns.sort(key=lambda col: node.get_expression_position(col[0]))
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
