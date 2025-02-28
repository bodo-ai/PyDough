"""
Logic for converting qualified DAG nodes to Relational nodes, using hybrid
nodes as an intermediary representation.
"""

__all__ = ["convert_ast_to_relational"]


from dataclasses import dataclass

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.metadata import (
    SimpleTableMetadata,
)
from pydough.qdag import (
    Calculate,
    CollectionAccess,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    Reference,
    SubCollection,
    TableCollection,
)
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnPruner,
    ColumnReference,
    CorrelatedReference,
    EmptySingleton,
    ExpressionSortInfo,
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
    WindowCallExpression,
)
from pydough.types import BooleanType, Int64Type, UnknownType

from .hybrid_decorrelater import run_hybrid_decorrelation
from .hybrid_tree import (
    ConnectionType,
    HybridBackRefExpr,
    HybridCalculate,
    HybridChildRefExpr,
    HybridCollation,
    HybridCollectionAccess,
    HybridColumnExpr,
    HybridConnection,
    HybridCorrelExpr,
    HybridExpr,
    HybridFilter,
    HybridFunctionExpr,
    HybridLimit,
    HybridLiteralExpr,
    HybridOperation,
    HybridPartition,
    HybridPartitionChild,
    HybridRefExpr,
    HybridRoot,
    HybridTranslator,
    HybridTree,
    HybridWindowExpr,
)


@dataclass
class TranslationOutput:
    """
    The output payload for the conversion of a HybridTree prefix to
    a Relational structure. Contains the Relational node tree in question,
    as well as a mapping that can be used to identify what column to use to
    access any HybridExpr's equivalent expression in the Relational node.
    """

    relational_node: RelationalNode
    """
    The relational tree describing the way to compute the answer for the
    logic originally in the hybrid tree.
    """

    expressions: dict[HybridExpr, ColumnReference]
    """
    A mapping of each expression that was accessible in the hybrid tree to the
    corresponding column reference in the relational tree that contains the
    value of that expression.
    """

    correlated_name: str | None = None
    """
    The name that can be used to refer to the relational output in correlated
    references.
    """


class RelTranslation:
    def __init__(self):
        # An index used for creating fake column names
        self.dummy_idx = 1
        # A stack of contexts used to point to ancestors for correlated
        # references.
        self.stack: list[TranslationOutput] = []

    def make_null_column(self, relation: RelationalNode) -> ColumnReference:
        """
        Inserts a new column into the relation whose value is NULL. If such a
        column already exists, it is used.

        Args:
            `relation`: the Relational node that the `NULL` term is being
            inserted into.

        Returns:
            A `ColumnReference` to the new/existing column of `relation` that
            is `NULL`.
        """
        name: str = f"NULL_{self.dummy_idx}"
        while True:
            if name not in relation.columns:
                break
            existing_val: RelationalExpression = relation.columns[name]
            if (
                isinstance(existing_val, LiteralExpression)
                and existing_val.value is None
            ):
                break
            self.dummy_idx += 1
            name = f"NULL_{self.dummy_idx}"
        relation.columns[name] = LiteralExpression(None, UnknownType())
        return ColumnReference(name, UnknownType())

    def get_column_name(
        self, name: str, existing_names: dict[str, RelationalExpression]
    ) -> str:
        """
        Replaces a name for a new column with another name if the name is
        already being used.

        Args:
            `name`: the name of the column to be replaced.
            `existing_names`: the dictionary of existing column names that
            are already being used in the current relational tree.

        Returns:
            A string based on `name` that is not part of `existing_names`.
        """
        new_name: str = name
        while new_name in existing_names:
            self.dummy_idx += 1
            new_name = f"{name}_{self.dummy_idx}"
        return new_name

    def get_correlated_name(self, context: TranslationOutput) -> str:
        """
        Finds the name used to refer to a context for correlated variable
        access. If the context does not have a correlated name, a new one is
        generated for it.

        Args:
            `context`: the context containing the relational subtree being
            referrenced in a correlated variable access.

        Returns:
            The name used to refer to the context in a correlated reference.
        """
        if context.correlated_name is None:
            context.correlated_name = f"corr{self.dummy_idx}"
            self.dummy_idx += 1
        return context.correlated_name

    def translate_expression(
        self, expr: HybridExpr, context: TranslationOutput | None
    ) -> RelationalExpression:
        """
        Converts a HybridExpr to a RelationalExpression node based on the
        current context. NOTE: currently only supported for literals, columns,
        and column references.

        Args:
            `expr`: the HybridExpr node to be converted.
            `context`: the data structure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts. Can be omitted in certain contexts, such as
            when deriving a table scan or literal.

        Returns:
            The converted relational expression.
        """
        inputs: list[RelationalExpression] = []
        match expr:
            case HybridColumnExpr():
                return ColumnReference(
                    expr.column.column_property.column_name, expr.typ
                )
            case HybridLiteralExpr():
                return LiteralExpression(expr.literal.value, expr.typ)
            case HybridRefExpr() | HybridChildRefExpr() | HybridBackRefExpr():
                assert context is not None
                if expr not in context.expressions:
                    if isinstance(expr, HybridRefExpr):
                        for back_expr in context.expressions:
                            if (
                                isinstance(back_expr, HybridBackRefExpr)
                                and back_expr.name == expr.name
                            ):
                                return context.expressions[back_expr]
                    raise ValueError(f"Context does not contain expression {expr}")
                return context.expressions[expr]
            case HybridFunctionExpr():
                inputs = [self.translate_expression(arg, context) for arg in expr.args]
                return CallExpression(expr.operator, expr.typ, inputs)
            case HybridWindowExpr():
                inputs = [self.translate_expression(arg, context) for arg in expr.args]
                partition_inputs = [
                    self.translate_expression(arg, context)
                    for arg in expr.partition_args
                ]
                order_inputs = [
                    ExpressionSortInfo(
                        self.translate_expression(arg.expr, context),
                        arg.asc,
                        arg.na_first,
                    )
                    for arg in expr.order_args
                ]
                return WindowCallExpression(
                    expr.window_func,
                    expr.typ,
                    inputs,
                    partition_inputs,
                    order_inputs,
                    expr.kwargs,
                )
            case HybridCorrelExpr():
                # Convert correlated expressions by converting the expression
                # they point to in the context of the top of the stack, then
                # wrapping the result in a correlated reference.
                ancestor_context: TranslationOutput = self.stack.pop()
                ancestor_expr: RelationalExpression = self.translate_expression(
                    expr.expr, ancestor_context
                )
                self.stack.append(ancestor_context)
                match ancestor_expr:
                    case ColumnReference():
                        return CorrelatedReference(
                            ancestor_expr.name,
                            self.get_correlated_name(ancestor_context),
                            expr.typ,
                        )
                    case CorrelatedReference():
                        return ancestor_expr
                    case _:
                        raise ValueError(
                            f"Unsupported expression to reference in a correlated reference: {ancestor_expr}"
                        )
            case _:
                raise NotImplementedError(
                    f"TODO: support relational conversion on {expr.__class__.__name__}"
                )

    def join_outputs(
        self,
        lhs_result: TranslationOutput,
        rhs_result: TranslationOutput,
        join_type: JoinType,
        join_keys: list[tuple[HybridExpr, HybridExpr]],
        child_idx: int | None,
    ) -> TranslationOutput:
        """
        Handles the joining of a parent context onto a child context.

        Args:
            `lhs_result`: the TranslationOutput payload storing containing the
            relational structure for the parent context.
            `rhs_result`: the TranslationOutput payload storing containing the
            relational structure for the child context.
            `join_type` the type of join to be used to connect `lhs_result`
            onto `rhs_result`.
            `join_keys`: a list of tuples in the form `(lhs_key, rhs_key)` that
            represent the equi-join keys used for the join from either side.
            `child_idx`: if None, means that the join is being used to step
            down from a parent into its child. If non-none, it means the join
            is being used to bring a child's elements into the same context as
            the parent, and the `child_idx` is the index of that child.

        Returns:
            The TranslationOutput payload containing the relational structure
            created by joining `lhs_result` and `lhs_result` in the manner
            described.
        """
        out_columns: dict[HybridExpr, ColumnReference] = {}
        join_columns: dict[str, RelationalExpression] = {}

        # Special case: if the lhs is an EmptySingleton, just return the RHS,
        # decorated if needed.
        if isinstance(lhs_result.relational_node, EmptySingleton):
            if child_idx is None:
                return rhs_result
            else:
                for expr, col_ref in rhs_result.expressions.items():
                    if isinstance(expr, HybridRefExpr):
                        child_ref: HybridExpr = HybridChildRefExpr(
                            expr.name, child_idx, expr.typ
                        )
                        out_columns[child_ref] = col_ref
                return TranslationOutput(rhs_result.relational_node, out_columns)

        # Create the join node so we know what aliases it uses, but leave
        # the condition as always-True and the output columns empty for now.
        # The condition & output columns will be filled in later.
        out_rel: Join = Join(
            [lhs_result.relational_node, rhs_result.relational_node],
            [LiteralExpression(True, BooleanType())],
            [join_type],
            join_columns,
            correl_name=lhs_result.correlated_name,
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
                shifted_expr: HybridExpr | None = expr.shift_back(1)
                if shifted_expr is not None:
                    out_columns[shifted_expr] = existing_ref
            else:
                out_columns[expr] = existing_ref

        # Skip the following steps for semi/anti joins
        if join_type not in (JoinType.SEMI, JoinType.ANTI):
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

    def apply_aggregations(
        self,
        connection: HybridConnection,
        context: TranslationOutput,
        agg_keys: list[HybridExpr],
    ) -> TranslationOutput:
        """
        Transforms the TranslationOutput payload from translating the
        subtree of HyrbidConnection by grouping it using the specified
        aggregation keys then deriving the aggregations in the `aggs` mapping
        of the HybridAggregation.

        Args:
            `connection`: the HybridConnection whose subtree is being derived.
            This connection must be of an aggregation type.
            `context`: the TranslationOutput being augmented.
            `agg_keys`: the list of expressions corresponding to the keys
            that should be used to aggregate `context`.

        Returns:
            The TranslationOutput payload for `context` wrapped in an
            aggregation.
        """
        assert connection.connection_type in (
            ConnectionType.AGGREGATION,
            ConnectionType.AGGREGATION_ONLY_MATCH,
            ConnectionType.NO_MATCH_AGGREGATION,
        )
        out_columns: dict[HybridExpr, ColumnReference] = {}
        keys: dict[str, ColumnReference] = {}
        aggregations: dict[str, CallExpression] = {}
        # First, propagate all key columns into the output, and add them to
        # the keys mapping of the aggregate.
        for agg_key in agg_keys:
            agg_key_expr = self.translate_expression(agg_key, context)
            assert isinstance(agg_key_expr, ColumnReference)
            out_columns[agg_key] = agg_key_expr
            keys[agg_key_expr.name] = agg_key_expr
        # Then, add all of the agg calls to the aggregations mapping of the
        # the aggregate, and add references to the corresponding dummy-names
        # to the output.
        for agg_name, agg_func in connection.aggs.items():
            assert agg_name not in keys
            col_ref: ColumnReference = ColumnReference(agg_name, agg_func.typ)
            hybrid_expr: HybridExpr = HybridRefExpr(agg_name, agg_func.typ)
            out_columns[hybrid_expr] = col_ref
            args: list[RelationalExpression] = [
                self.translate_expression(arg, context) for arg in agg_func.args
            ]
            aggregations[agg_name] = CallExpression(
                agg_func.operator, agg_func.typ, args
            )
        out_rel: RelationalNode = Aggregate(context.relational_node, keys, aggregations)
        return TranslationOutput(out_rel, out_columns)

    def handle_children(
        self, context: TranslationOutput, hybrid: HybridTree, pipeline_idx: int
    ) -> TranslationOutput:
        """
        Post-processes a TranslationOutput payload by finding any children of
        the current hybrid tree level that are newly able to be defined, and
        bringing them into context via the handler.

        Args:
            `context`: the TranslationOutput being augmented, if one exists.
            `hybrid`: the current level of the HybridTree.
            `pipeline_idx`: the index of the element in the pipeline of
            `hybrid` that has just been defined, meaning any children that
            depend on it can now also be defined.

        Returns:
            The augmented version of `context` with any children that are
            possible to define brought into context.
        """
        for child_idx, child in enumerate(hybrid.children):
            if child.required_steps == pipeline_idx:
                self.stack.append(context)
                child_output = self.rel_translation(
                    child, child.subtree, len(child.subtree.pipeline) - 1
                )
                self.stack.pop()
                assert child.subtree.join_keys is not None
                join_keys: list[tuple[HybridExpr, HybridExpr]] = child.subtree.join_keys
                agg_keys: list[HybridExpr]
                if child.subtree.agg_keys is None:
                    agg_keys = [rhs_key for _, rhs_key in join_keys]
                else:
                    agg_keys = child.subtree.agg_keys
                child_expr: HybridExpr
                match child.connection_type:
                    case (
                        ConnectionType.SINGULAR
                        | ConnectionType.SINGULAR_ONLY_MATCH
                        | ConnectionType.AGGREGATION
                        | ConnectionType.AGGREGATION_ONLY_MATCH
                        | ConnectionType.SEMI
                        | ConnectionType.ANTI
                    ):
                        if child.connection_type.is_aggregation:
                            child_output = self.apply_aggregations(
                                child, child_output, agg_keys
                            )
                        context = self.join_outputs(
                            context,
                            child_output,
                            child.connection_type.join_type,
                            join_keys,
                            child_idx,
                        )
                    case (
                        ConnectionType.NO_MATCH_SINGULAR
                        | ConnectionType.NO_MATCH_AGGREGATION
                    ):
                        assert child_idx is not None
                        context = self.join_outputs(
                            context,
                            child_output,
                            child.connection_type.join_type,
                            join_keys,
                            child_idx,
                        )
                        # Map every child_idx reference from child_output to null
                        null_column: ColumnReference = self.make_null_column(
                            context.relational_node
                        )
                        for expr in child_output.expressions:
                            if isinstance(expr, HybridRefExpr):
                                child_expr = HybridChildRefExpr(
                                    expr.name, child_idx, expr.typ
                                )
                                context.expressions[child_expr] = null_column
                        # For aggregations, map every child_idx reference to the
                        # `aggs` list to null
                        if child.connection_type == ConnectionType.NO_MATCH_AGGREGATION:
                            for agg_name, agg_expr in child.aggs.items():
                                child_expr = HybridChildRefExpr(
                                    agg_name, child_idx, agg_expr.typ
                                )
                                context.expressions[child_expr] = null_column
                    case conn_type:
                        raise ValueError(f"Unsupported connection type {conn_type}")
        return context

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
            `context`: the data structure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts.

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

        join_keys: list[tuple[HybridExpr, HybridExpr]] = (
            HybridTranslator.get_subcollection_join_keys(
                collection_access.subcollection_property,
                parent.pipeline[-1],
                node,
            )
        )

        return self.join_outputs(
            context,
            rhs_output,
            JoinType.INNER,
            join_keys,
            None,
        )

    def translate_child_sub_collection(
        self, node: HybridCollectionAccess
    ) -> TranslationOutput:
        """
        Converts a subcollection access, used as the root of a child subtree,
        into a table scan.

        Args:
            `connection`: the HybridConnection linking the parent context
            to the child subtree.
            `node`: the collection access that is the root of the child
            subtree.

        Returns:
            The TranslationOutput payload corresponding to the access of the
            child collection.
        """
        # First, build the table scan for the collection being stepped into.
        collection_access: CollectionAccess = node.collection
        assert isinstance(collection_access, SubCollection)
        assert isinstance(
            collection_access.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {collection_access.collection.__class__.__name__}"
        result: TranslationOutput = self.build_simple_table_scan(node)
        return result

    def translate_partition(
        self,
        node: HybridPartition,
        context: TranslationOutput,
        hybrid: HybridTree,
        pipeline_idx: int,
    ) -> TranslationOutput:
        """
        Converts a partition into the correct context with access to the
        aggregated child inputs.

        Args:
            `node`: the node corresponding to the partition being derived.
            `context`: the data structure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts.
            `hybrid`: the current level of the hybrid tree to be derived,
            including all levels before it.
            `pipeline_idx`: the index of the operation in the pipeline of the
            current level that is to be derived, as well as all operations
            preceding it in the pipeline.

        Returns:
            The TranslationOutput payload containing access to the aggregated
            child corresponding to the partition data.
        """
        expressions: dict[HybridExpr, ColumnReference] = {}
        # Account for the fact that the PARTITION is stepping down a level,
        # without actually joining.
        for expr, ref in context.expressions.items():
            shifted_expr: HybridExpr | None = expr.shift_back(1)
            if shifted_expr is not None:
                expressions[shifted_expr] = ref
        result: TranslationOutput = TranslationOutput(
            context.relational_node, expressions
        )
        result = self.handle_children(result, hybrid, pipeline_idx)
        # Pull every aggregation key into the current context since it is now
        # accessible as a normal ref instead of a child ref.
        for key_name in node.key_names:
            key_expr = node.terms[key_name]
            assert isinstance(key_expr, HybridChildRefExpr)
            hybrid_ref: HybridRefExpr = HybridRefExpr(key_expr.name, key_expr.typ)
            result.expressions[hybrid_ref] = result.expressions[key_expr]
        return result

    def translate_filter(
        self,
        node: HybridFilter,
        context: TranslationOutput,
    ) -> TranslationOutput:
        """
        Converts a filter into a relational Filter node on top of its child.

        Args:
            `node`: the node corresponding to the filter being derived.
            `context`: the data structure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts.

        Returns:
            The TranslationOutput payload containing a FILTER on top of
            the relational node for the parent to derive any additional terms.
        """
        # Keep all existing columns.
        kept_columns: dict[str, RelationalExpression] = {
            name: ColumnReference(name, context.relational_node.columns[name].data_type)
            for name in context.relational_node.columns
        }
        condition: RelationalExpression = self.translate_expression(
            node.condition, context
        )
        out_rel: Filter = Filter(context.relational_node, condition, kept_columns)
        return TranslationOutput(out_rel, context.expressions)

    def translate_limit(
        self,
        node: HybridLimit,
        context: TranslationOutput,
    ) -> TranslationOutput:
        """
        Converts a HybridLimit into a relational Limit node on top of its child.

        Args:
            `node`: the node corresponding to the limit being derived.
            `context`: the data structure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts. Can be omitted in certain contexts, such as
            when deriving a table scan or literal.

        Returns:
            The TranslationOutput payload containing a Limit on top of
            the relational node for the parent to derive any additional terms.
        """
        # Keep all existing columns.
        kept_columns: dict[str, RelationalExpression] = {
            name: ColumnReference(name, context.relational_node.columns[name].data_type)
            for name in context.relational_node.columns
        }
        limit_expr: LiteralExpression = LiteralExpression(
            node.records_to_keep, Int64Type()
        )
        orderings: list[ExpressionSortInfo] = make_relational_ordering(
            node.orderings, context.expressions
        )
        out_rel: Limit = Limit(
            context.relational_node, limit_expr, kept_columns, orderings
        )
        return TranslationOutput(out_rel, context.expressions)

    def translate_calculate(
        self,
        node: HybridCalculate,
        context: TranslationOutput,
    ) -> TranslationOutput:
        """
        Converts a CALCULATE into a project on top of its child to derive
        additional terms.

        Args:
            `node`: the node corresponding to the CALCULATE being derived.
            `context`: the data structure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts and the corresponding relational node.

        Returns:
            The TranslationOutput payload containing a PROJECT that propagates
            any existing terms top of
            the relational node for the parent to derive any additional terms.
        """
        proj_columns: dict[str, RelationalExpression] = {}
        out_columns: dict[HybridExpr, ColumnReference] = {}
        # Propagate all of the existing columns.
        for name in context.relational_node.columns:
            proj_columns[name] = ColumnReference(
                name, context.relational_node.columns[name].data_type
            )
        for expr in context.expressions:
            out_columns[expr] = context.expressions[expr].with_input(None)
        # Populate every expression into the project's columns by translating
        # it relative to the input context.
        for name in node.new_expressions:
            name = node.renamings.get(name, name)
            hybrid_expr: HybridExpr = node.terms[name]
            ref_expr: HybridRefExpr = HybridRefExpr(name, hybrid_expr.typ)
            rel_expr: RelationalExpression = self.translate_expression(
                hybrid_expr, context
            )
            # Ensure the name of the new column is not already being used. If
            # it is, choose a new name. The new name will be the original name
            # with a numerical index appended to it.
            if name in proj_columns and proj_columns[name] != rel_expr:
                name = self.get_column_name(name, proj_columns)
            proj_columns[name] = rel_expr
            out_columns[ref_expr] = ColumnReference(name, rel_expr.data_type)
        out_rel: Project = Project(context.relational_node, proj_columns)
        return TranslationOutput(out_rel, out_columns)

    def translate_partition_child(
        self,
        node: HybridPartitionChild,
        context: TranslationOutput,
    ) -> TranslationOutput:
        """
        Converts a step into the child of a PARTITION node into a join between
        the aggregated partitions versus the data that was originally being
        partitioned.

        Args:
            `node`: the node corresponding to the partition child access.
            `parent`: the hybrid tree of the previous layer that the access
            steps down from.
            `context`: the data structure storing information used by the
            conversion, such as bindings of already translated terms from
            preceding contexts.

        Returns:
            The TranslationOutput payload containing expressions for both the
            aggregated partitions and the original partitioned data.
        """
        child_output: TranslationOutput = self.rel_translation(
            None, node.subtree, len(node.subtree.pipeline) - 1
        )
        join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        assert node.subtree.agg_keys is not None
        for agg_key in sorted(node.subtree.agg_keys, key=str):
            join_keys.append((agg_key, agg_key))

        result = self.join_outputs(
            context,
            child_output,
            JoinType.INNER,
            join_keys,
            None,
        )
        return result

    def rel_translation(
        self,
        connection: HybridConnection | None,
        hybrid: HybridTree,
        pipeline_idx: int,
    ) -> TranslationOutput:
        """
        The recursive procedure for converting a prefix of the hybrid tree
        into a TranslationOutput payload.

        Args:
            `connection`: the HybridConnection instance that defines the
            parent-child relationship containing the subtree being defined
            (as the child), or None if this is the main path.
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

        # Identify the operation that will be computed at this stage, and the
        # previous stage on the current level of the hybrid tree, or the last
        # operation from the preceding level if we are at the start of the
        # current level. However, one may not exist, in which case the current
        # stage must be defined as the first step.
        operation: HybridOperation = hybrid.pipeline[pipeline_idx]
        result: TranslationOutput
        preceding_hybrid: tuple[HybridTree, int] | None = None
        if pipeline_idx > 0:
            preceding_hybrid = (hybrid, pipeline_idx - 1)
        elif hybrid.parent is not None:
            preceding_hybrid = (hybrid.parent, len(hybrid.parent.pipeline) - 1)

        # First, recursively fetch the TranslationOutput of the preceding
        # stage, if valid.
        context: TranslationOutput | None
        if preceding_hybrid is None:
            context = None
        elif isinstance(preceding_hybrid[0].pipeline[preceding_hybrid[1]], HybridRoot):
            # If at the true root, set the starting context to just be a dummy
            # VALUES clause.
            context = TranslationOutput(EmptySingleton(), {})
            context = self.handle_children(context, *preceding_hybrid)
        else:
            context = self.rel_translation(connection, *preceding_hybrid)

        # Then, dispatch onto the logic to transform from the context into the
        # new translation output.
        handled_children: bool = False
        match operation:
            case HybridCollectionAccess():
                if isinstance(operation.collection, TableCollection):
                    result = self.build_simple_table_scan(operation)
                    if context is not None:
                        # If the collection access is the child of something
                        # else, join it onto that something else. Use the
                        # uniqueness keys of the ancestor, which should also be
                        # present in the collection (e.g. joining a partition
                        # onto the original data using the partition keys).
                        assert preceding_hybrid is not None
                        join_keys: list[tuple[HybridExpr, HybridExpr]] = []
                        for unique_column in sorted(
                            preceding_hybrid[0].pipeline[0].unique_exprs, key=str
                        ):
                            if unique_column not in result.expressions:
                                raise ValueError(
                                    f"Cannot connect parent context to child {operation.collection} because {unique_column} is not in the child's expressions."
                                )
                            join_keys.append((unique_column, unique_column))
                        result = self.join_outputs(
                            context,
                            result,
                            JoinType.INNER,
                            join_keys,
                            None,
                        )
                else:
                    # For subcollection accesses, the access is either a step
                    # from a parent into a child (if the parent exists), or the
                    # root of a child subtree (if the parent does not exist).
                    if hybrid.parent is not None:
                        assert context is not None, "Malformed HybridTree pattern."
                        result = self.translate_sub_collection(
                            operation, hybrid.parent, context
                        )
                    else:
                        result = self.build_simple_table_scan(operation)
            case HybridPartitionChild():
                assert context is not None, "Malformed HybridTree pattern."
                result = self.translate_partition_child(operation, context)
            case HybridCalculate():
                assert context is not None, "Malformed HybridTree pattern."
                result = self.translate_calculate(operation, context)
            case HybridFilter():
                assert context is not None, "Malformed HybridTree pattern."
                result = self.translate_filter(operation, context)
            case HybridPartition():
                if context is None:
                    context = TranslationOutput(EmptySingleton(), {})
                result = self.translate_partition(
                    operation, context, hybrid, pipeline_idx
                )
                handled_children = True
            case HybridLimit():
                assert context is not None, "Malformed HybridTree pattern."
                result = self.translate_limit(operation, context)
            case _:
                raise NotImplementedError(
                    f"TODO: support relational conversion on {operation.__class__.__name__}"
                )
        if not handled_children:
            result = self.handle_children(result, hybrid, pipeline_idx)
        return result

    @staticmethod
    def preprocess_root(
        node: PyDoughCollectionQDAG,
        output_cols: list[tuple[str, str]] | None,
    ) -> PyDoughCollectionQDAG:
        """
        Transforms the final PyDough collection by appending it with an extra
        CALCULATE containing all of the columns that are output.
        """
        # Fetch all of the expressions that should be kept in the final output
        final_terms: list[tuple[str, PyDoughExpressionQDAG]] = []
        if output_cols is None:
            for name in node.calc_terms:
                final_terms.append((name, Reference(node, name)))
            final_terms.sort(key=lambda term: node.get_expression_position(term[0]))
        else:
            for _, column in output_cols:
                final_terms.append((column, Reference(node, column)))
        children: list[PyDoughCollectionQDAG] = []
        final_calc: Calculate = Calculate(node, children).with_terms(final_terms)
        return final_calc


def make_relational_ordering(
    collation: list[HybridCollation],
    expressions: dict[HybridExpr, ColumnReference],
) -> list[ExpressionSortInfo]:
    """
    Converts a list of collation expressions into a list of ExpressionSortInfo.

    Args:
        collation (list[CollationExpression]): The list of collation
            expressions to convert.
        expressions (dict[HybridExpr, ColumnReference]): The dictionary of
            expressions to use for the relational ordering.

    Returns:
        list[ExpressionSortInfo]: The ordering expressions converted into
        ExpressionSortInfo.
    """
    orderings: list[ExpressionSortInfo] = []
    for col_expr in collation:
        relational_expr: ColumnReference = expressions[col_expr.expr]
        collation_expr: ExpressionSortInfo = ExpressionSortInfo(
            relational_expr, col_expr.asc, col_expr.na_first
        )
        orderings.append(collation_expr)
    return orderings


def convert_ast_to_relational(
    node: PyDoughCollectionQDAG,
    columns: list[tuple[str, str]] | None,
    configs: PyDoughConfigs,
) -> RelationalRoot:
    """
    Main API for converting from the collection QDAG form into relational
    nodes.

    Args:
        `node`: the PyDough QDAG collection node to be translated.
        `columns`: a list of tuples in the form `(alias, column)`
        describing every column that should be in the output, in the order
        they should appear, and the alias they should be given. If None, uses
        the most recent CALCULATE in the node to determine the columns.
        `configs`: the configuration settings to use during translation.

    Returns:
        The RelationalRoot for the entire PyDough calculation that the
        collection node corresponds to. Ensures that the desired output columns
        of `node` are included in the root in the correct order, and if it
        has an ordering then the relational root stores that information.
    """
    # Pre-process the QDAG node so the final CALCULATE includes any ordering
    # keys.
    translator: RelTranslation = RelTranslation()
    node = translator.preprocess_root(node, columns)

    # Convert the QDAG node to the hybrid form, decorrelate it, then invoke
    # the relational conversion procedure. The first element in the returned
    # list is the final rel node.
    hybrid: HybridTree = HybridTranslator(configs).make_hybrid_tree(node, None)
    run_hybrid_decorrelation(hybrid)
    renamings: dict[str, str] = hybrid.pipeline[-1].renamings
    output: TranslationOutput = translator.rel_translation(
        None, hybrid, len(hybrid.pipeline) - 1
    )
    ordered_columns: list[tuple[str, RelationalExpression]] = []
    orderings: list[ExpressionSortInfo] | None = None

    # Extract the relevant expressions for the final columns and ordering keys
    # so that the root node can be built from them.
    hybrid_expr: HybridExpr
    rel_expr: RelationalExpression
    name: str
    original_name: str
    if columns is None:
        for original_name in node.calc_terms:
            name = renamings.get(original_name, original_name)
            hybrid_expr = hybrid.pipeline[-1].terms[name]
            rel_expr = output.expressions[hybrid_expr]
            ordered_columns.append((original_name, rel_expr))
        ordered_columns.sort(key=lambda col: node.get_expression_position(col[0]))
    else:
        for alias, column in columns:
            hybrid_expr = hybrid.pipeline[-1].terms[column]
            rel_expr = output.expressions[hybrid_expr]
            ordered_columns.append((alias, rel_expr))
    hybrid_orderings: list[HybridCollation] = hybrid.pipeline[-1].orderings
    if hybrid_orderings:
        orderings = make_relational_ordering(hybrid_orderings, output.expressions)
    unpruned_result: RelationalRoot = RelationalRoot(
        output.relational_node, ordered_columns, orderings
    )
    return ColumnPruner().prune_unused_columns(unpruned_result)
