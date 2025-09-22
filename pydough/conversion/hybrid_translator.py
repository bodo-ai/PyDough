"""
Definition of the logic to convert QDAG nodes into a HybridTree
"""

__all__ = ["HybridTranslator", "HybridTree"]

from collections.abc import Iterable

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseDialect
from pydough.errors import PyDoughSQLException
from pydough.metadata import (
    CartesianProductMetadata,
    GeneralJoinMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
)
from pydough.qdag import (
    BackReferenceExpression,
    Calculate,
    ChildOperator,
    ChildOperatorChildAccess,
    ChildReferenceCollection,
    ChildReferenceExpression,
    CollationExpression,
    ColumnProperty,
    ExpressionFunctionCall,
    GlobalContext,
    Literal,
    OrderBy,
    PartitionBy,
    PartitionChild,
    PartitionKey,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    Reference,
    SidedReference,
    Singular,
    SubCollection,
    TableCollection,
    TopK,
    Where,
    WindowCall,
)
from pydough.types import BooleanType, NumericType

from .hybrid_connection import ConnectionType, HybridConnection
from .hybrid_correlation_extraction import HybridCorrelationExtractor
from .hybrid_decorrelater import HybridDecorrelater
from .hybrid_expressions import (
    HybridBackRefExpr,
    HybridChildRefExpr,
    HybridCollation,
    HybridColumnExpr,
    HybridCorrelExpr,
    HybridExpr,
    HybridFunctionExpr,
    HybridLiteralExpr,
    HybridRefExpr,
    HybridSidedRefExpr,
    HybridWindowExpr,
)
from .hybrid_operations import (
    HybridCalculate,
    HybridCollectionAccess,
    HybridFilter,
    HybridLimit,
    HybridNoop,
    HybridOperation,
    HybridPartition,
    HybridPartitionChild,
    HybridRoot,
)
from .hybrid_syncretizer import HybridSyncretizer
from .hybrid_tree import HybridTree


class HybridTranslator:
    """
    Class used to translate PyDough QDAG nodes into the HybridTree structure.
    """

    def __init__(self, configs: PyDoughConfigs, dialect: DatabaseDialect):
        self.configs = configs
        # An index used for creating fake column names for aliases
        self.alias_counter: int = 0
        # A stack where each element is a hybrid tree being derived
        # as as subtree of the previous element, and the current tree is
        # being derived as the subtree of the last element.
        self.stack: list[HybridTree] = []
        # If True, rewrites MEDIAN calls into an average of the 1-2 median rows
        # or rewrites QUANTILE calls to select the first qualifying row,
        # both derived from window functions, otherwise leaves as-is.
        self.rewrite_median_quantile: bool = dialect not in {
            DatabaseDialect.ANSI,
            DatabaseDialect.SNOWFLAKE,
            DatabaseDialect.POSTGRES,
        }

    @staticmethod
    def get_subcollection_join_keys(
        subcollection_property: SubcollectionRelationshipMetadata,
        parent_node: HybridOperation,
        child_node: HybridOperation,
    ) -> list[tuple[HybridExpr, HybridExpr]]:
        """
        Fetches the list of pairs of keys used to join a parent node onto its
        child node

        Args:
            `subcollection_property`: the metadata for the subcollection
            access.
            `parent_node`: the HybridOperation node corresponding to the parent.
            `child_node`: the HybridOperation node corresponding to the access.

        Returns:
            A list of tuples in the form `(lhs_key, rhs_key)` where each
            `lhs_key` is the join key from the parent's perspective and each
            `rhs_key` is the join key from the child's perspective.
        """
        join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        if isinstance(subcollection_property, SimpleJoinMetadata):
            # If the subcollection is a simple join property, extract the keys
            # and build the corresponding (lhs_key == rhs_key) conditions
            for lhs_name in subcollection_property.keys:
                lhs_key: HybridExpr = parent_node.get_term_as_ref(lhs_name)
                for rhs_name in subcollection_property.keys[lhs_name]:
                    rhs_key: HybridExpr = child_node.get_term_as_ref(rhs_name)
                    join_keys.append((lhs_key, rhs_key))
        elif not isinstance(subcollection_property, CartesianProductMetadata):
            raise NotImplementedError(
                f"Unsupported subcollection property type used for accessing a subcollection: {subcollection_property.__class__.__name__}"
            )
        return join_keys

    @staticmethod
    def identify_connection_types(
        expr: PyDoughExpressionQDAG,
        child_idx: int,
        reference_types: set[ConnectionType],
        inside_aggregation: bool = False,
    ) -> None:
        """
        Recursively identifies what types ways a child collection is referenced
        by its parent context.

        Args:
            `expr`: the expression being recursively checked for references
            to the child collection.
            `child_idx`: the index of the child that is being searched for
            references to it.
            `reference_types`: the set of known connection types that the
            are used when referencing the child; the function should mutate
            this set if it finds any new connections.
            `inside_aggregation`: True if `expr` is inside of a call to an
            aggregation function.
        """
        match expr:
            # If `expr` is a reference to the child in question, add
            # a reference that is either singular or aggregation depending
            # on the `inside_aggregation` argument
            case ChildReferenceExpression() if expr.child_idx == child_idx:
                reference_types.add(
                    ConnectionType.AGGREGATION
                    if inside_aggregation
                    else ConnectionType.SINGULAR
                )
            case WindowCall():
                # Otherwise, mutate `reference_types` based on the arguments
                # to the window call.
                for window_arg in expr.args:
                    HybridTranslator.identify_connection_types(
                        window_arg, child_idx, reference_types, inside_aggregation
                    )
                for col in expr.collation_args:
                    HybridTranslator.identify_connection_types(
                        col.expr, child_idx, reference_types, inside_aggregation
                    )
            case ExpressionFunctionCall():
                # If `expr` is a `HAS` call on the child in question, add a
                # semi-join connection.
                if expr.operator == pydop.HAS:
                    arg = expr.args[0]
                    assert isinstance(arg, ChildReferenceCollection)
                    if arg.child_idx == child_idx:
                        reference_types.add(ConnectionType.SEMI)
                # If `expr` is a `HASNOT` call on the child in question, add a
                # anti-join connection.
                elif expr.operator == pydop.HASNOT:
                    arg = expr.args[0]
                    assert isinstance(arg, ChildReferenceCollection)
                    if arg.child_idx == child_idx:
                        reference_types.add(ConnectionType.ANTI)
                # Otherwise, mutate `reference_types` based on the arguments
                # to the function call.
                else:
                    for arg in expr.args:
                        if isinstance(arg, ChildReferenceCollection):
                            # If the argument is a reference to a child,
                            # collection, e.g. `COUNT(X)`, treat as an
                            # aggregation reference if it refers to the child
                            # in question.
                            if arg.child_idx == child_idx:
                                reference_types.add(ConnectionType.AGGREGATION)
                        else:
                            # Otherwise, recursively check the arguments to the
                            # function, promoting `inside_aggregation` to True
                            # if the function is an aggfunc.
                            assert isinstance(arg, PyDoughExpressionQDAG)
                            inside_aggregation = (
                                inside_aggregation or expr.operator.is_aggregation
                            )
                            HybridTranslator.identify_connection_types(
                                arg, child_idx, reference_types, inside_aggregation
                            )
            case _:
                return

    def inject_expression(
        self, hybrid: HybridTree, expr: HybridExpr, create_new_calc: bool
    ) -> HybridExpr:
        """
        Injects a hybrid expression into the HybridTree's terms, returning
        the new name it was injected with.

        Args:
            `hybrid`: the base of the HybridTree to inject the expression into.
            `expr`: the expression to be injected.
            `create_new_calc`: if True, injects the expression into a new
            CALCULATE operation. If False, injects the expression into the
            last CALCULATE operation in the pipeline, if there is one at the
            end, otherwise creates a new one.

        Returns:
            The HybridExpr corresponding to the injected expression.
        """
        name: str = self.get_internal_name("expr", [hybrid.pipeline[-1].terms])
        if isinstance(hybrid.pipeline[-1], HybridCalculate) and not create_new_calc:
            hybrid.pipeline[-1].terms[name] = expr
            hybrid.pipeline[-1].new_expressions[name] = expr
        else:
            hybrid.add_operation(
                HybridCalculate(
                    hybrid.pipeline[-1],
                    {name: expr},
                    hybrid.pipeline[-1].orderings,
                )
            )
        return HybridRefExpr(name, expr.typ)

    def eject_aggregate_inputs(self, hybrid: HybridTree) -> None:
        """
        Ensures that any inputs to aggregation calls are only references to
        columns from the subtree of the hybrid connection being aggregated.

        Args:
            `hybrid`: the base of the HybridTree to eject the aggregate inputs
            from. The ancestors & children of `hybrid` must also be processed.
        """
        # Recursively eject inputs from the ancestors & children
        if hybrid.parent is not None:
            self.eject_aggregate_inputs(hybrid.parent)
        for child in hybrid.children:
            self.eject_aggregate_inputs(child.subtree)
            create_new_calc: bool = True
            # For each child, look through each of its aggregation calls and
            # see if any of their arguments are not references. If so, eject
            # those expressions into a calculate in the child subtree, and
            # replace the argument with a reference to the new expression.
            for agg_name, agg_call in sorted(child.aggs.items()):
                rewritten: bool = False
                new_args: list[HybridExpr] = []
                for arg in agg_call.args:
                    if isinstance(arg, (HybridRefExpr, HybridLiteralExpr)):
                        new_args.append(arg)
                    else:
                        rewritten = True
                        new_args.append(
                            self.inject_expression(child.subtree, arg, create_new_calc)
                        )
                        create_new_calc = False
                if rewritten:
                    child.aggs[agg_name] = HybridFunctionExpr(
                        agg_call.operator,
                        new_args,
                        agg_call.typ,
                    )

    def run_rewrites(self, hybrid: HybridTree):
        """
        Run any rewrite procedures that must occur after de-correlation, such
        as converting MEDIAN to an average of the 1-2 median rows. Also converting
        the QUANTILE calls to the appropriate window function calls.


        Args:
            `hybrid`: the bottom of the hybrid tree to rewrite.
        """
        # Recursively proceed on the ancestors & children
        if hybrid.parent is not None:
            self.run_rewrites(hybrid.parent)
        for child in hybrid.children:
            self.run_rewrites(child.subtree)

        create_new_calc: bool = True
        # Rewrite any MEDIAN and QUANTILE calls
        if self.rewrite_median_quantile:
            for child in hybrid.children:
                for agg_name, agg_call in child.aggs.items():
                    if agg_call.operator == pydop.MEDIAN:
                        child.aggs[agg_name] = self.rewrite_median_call(
                            child, agg_call, create_new_calc
                        )
                        create_new_calc = False
                    if agg_call.operator == pydop.QUANTILE:
                        child.aggs[agg_name] = self.rewrite_quantile_call(
                            child, agg_call, create_new_calc
                        )
                        create_new_calc = False

    def qdag_expr_contains_window(self, expr: PyDoughExpressionQDAG) -> bool:
        """
        Checks if the given QDAG expression contains a window function call.

        Args:
            `expr`: the QDAG expression to check.

        Returns:
            True if the expression contains a window function call, False
            otherwise.
        """
        match expr:
            case WindowCall():
                return True
            case ExpressionFunctionCall():
                return any(
                    isinstance(arg, PyDoughExpressionQDAG)
                    and self.qdag_expr_contains_window(arg)
                    for arg in expr.args
                )
            case _:
                return False

    def populate_children(
        self,
        hybrid: HybridTree,
        child_operator: ChildOperator,
        child_idx_mapping: dict[int, int],
    ) -> None:
        """
        Helper utility that takes any children of a child operator (CALCULATE,
        WHERE, etc.) and builds the corresponding HybridTree subtree,
        where the parent of the subtree's root is absent instead of the
        current level, and inserts the corresponding HybridConnection node.

        Args:
            `hybrid`: the HybridTree having children added to it.
            `child_operator`: the collection QDAG node (CALCULATE, WHERE, etc.)
            containing the children.
            `child_idx_mapping`: a mapping of indices of children of the
            original `child_operator` to the indices of children of the hybrid
            tree level, since the hybrid tree contains the children of all
            pipeline operators of the current level and therefore the indices
            get changes. When the child is inserted, this mapping is mutated
            accordingly so expressions using the child indices know what hybrid
            connection index to use.
        """
        self.stack.append(hybrid)
        for child_idx, child in enumerate(child_operator.children):
            # Infer how the child is used by the current operation based on
            # the expressions that the operator uses.
            reference_types: set[ConnectionType] = set()
            cannot_filter: bool = False
            match child_operator:
                case Where():
                    self.identify_connection_types(
                        child_operator.condition, child_idx, reference_types
                    )
                    cannot_filter = self.qdag_expr_contains_window(
                        child_operator.condition
                    )
                case OrderBy():
                    for col in child_operator.collation:
                        self.identify_connection_types(
                            col.expr, child_idx, reference_types
                        )
                case Calculate():
                    for expr in child_operator.calc_term_values.values():
                        self.identify_connection_types(expr, child_idx, reference_types)
                        cannot_filter |= self.qdag_expr_contains_window(expr)
                case PartitionBy():
                    reference_types.add(ConnectionType.AGGREGATION)
            # Combine the various references to the child to identify the type
            # of connection and add the child. If it already exists, the index
            # of the existing child will be used instead, but the connection
            # type will be updated to reflect the new invocation of the child.
            if len(reference_types) == 0:
                raise ValueError(
                    f"Bad call to populate_children: child {child_idx} of {child_operator} is never used"
                )
            connection_type: ConnectionType = reference_types.pop()
            for con_typ in reference_types:
                connection_type = connection_type.reconcile_connection_types(con_typ)
            # Build the hybrid tree for the child. Before doing so, reset the
            # alias counter to 0 to ensure that identical subtrees are named
            # in the same manner. Afterwards, reset the alias counter to its
            # value within this context.
            snapshot: int = self.alias_counter
            self.alias_counter = 0
            subtree: HybridTree = self.make_hybrid_tree(
                child,
                hybrid,
                connection_type.is_aggregation
                or connection_type == ConnectionType.SEMI,
            )
            back_exprs: dict[str, HybridExpr] = {}
            for name in subtree.ancestral_mapping:
                # Skip adding backrefs for terms that remain part of the
                # ancestry through the PARTITION, since this creates an
                # unecessary correlation.
                if (
                    name in hybrid.ancestral_mapping
                    or name in hybrid.pipeline[-1].terms
                    or subtree.ancestral_mapping[name] == 0
                ):
                    continue
                hybrid_back_expr = self.make_hybrid_expr(
                    subtree,
                    child.get_expr(name),
                    {},
                    False,
                )
                back_exprs[name] = hybrid_back_expr
            if len(back_exprs):
                subtree.add_operation(
                    HybridCalculate(
                        subtree.pipeline[-1],
                        back_exprs,
                        subtree.pipeline[-1].orderings,
                    )
                )
            self.alias_counter = snapshot
            # If the subtree is guaranteed to exist with regards to the current
            # context, promote it by reconciling with SEMI so the logic will
            # not worry about trying to maintain records of the parent even
            # when the child does not exist.
            if (not connection_type.is_anti) and subtree.always_exists():
                connection_type = connection_type.reconcile_connection_types(
                    ConnectionType.SEMI
                )
            min_idx: int = hybrid.get_min_child_idx(subtree, connection_type)
            child_idx_mapping[child_idx] = hybrid.add_child(
                subtree,
                connection_type,
                min_idx,
                len(hybrid.pipeline),
                cannot_filter,
            )
        self.stack.pop()

    def postprocess_agg_output(
        self, agg_call: HybridFunctionExpr, agg_ref: HybridExpr, joins_can_nullify: bool
    ) -> HybridExpr:
        """
        Transforms an aggregation function call in any ways that are necessary
        due to configs, such as coalescing the output with zero.

        Args:
            `agg_call`: the aggregation call whose reference must be
            transformed if the configs demand it.
            `agg_ref`: the reference to the aggregation call that is
            transformed if the configs demand it.
            `joins_can_nullify`: True if the aggregation is fed into a left
            join, which creates the requirement for some aggregations like
            `COUNT` to have their defaults replaced.

        Returns:
            The transformed version of `agg_ref`, if postprocessing is required.
        """
        # If doing a SUM or AVG, and the configs are set to default those
        # functions to zero when there are no values, decorate the result
        # with `DEFAULT_TO(x, 0)`. Also, always does this step with
        # COUNT/NDISTINCT for left joins since the semantics of those functions
        # never allow returning NULL.
        if (
            (agg_call.operator == pydop.SUM and self.configs.sum_default_zero)
            or (agg_call.operator == pydop.AVG and self.configs.avg_default_zero)
            or (
                agg_call.operator in (pydop.COUNT, pydop.NDISTINCT)
                and joins_can_nullify
            )
        ):
            agg_ref = HybridFunctionExpr(
                pydop.DEFAULT_TO,
                [agg_ref, HybridLiteralExpr(Literal(0, NumericType()))],
                agg_call.typ,
            )
        return agg_ref

    def gen_agg_name(self, connection: "HybridConnection") -> str:
        """
        Generates a unique name for an aggregation function's output that
        is not already used.

        Args:
            `connection`: the HybridConnection in which the aggregation
            is being defined. The name cannot overlap with any other agg
            names or term names of the connection.

        Returns:
            The new name to be used.
        """
        return self.get_internal_name(
            "agg", [connection.subtree.pipeline[-1].terms, connection.aggs]
        )

    def get_ordering_name(self, hybrid: HybridTree) -> str:
        return self.get_internal_name("ordering", [hybrid.pipeline[-1].terms])

    def get_internal_name(
        self, prefix: str, reserved_names: list[Iterable[str]]
    ) -> str:
        """
        Generates a name to be used in the terms of a HybridTree with a
        specified prefix that does not overlap with certain names that have
        already been taken in that context.

        Args:
            `prefix`: the prefix that the generated name should start with.
            `reserved_names`: a list of mappings where the keys in each mapping
            are names that cannot be used because they have already been taken.

        Returns:
            The string of the name chosen with the corresponding prefix that
            does not overlap with the reserved name.
        """
        name = f"{prefix}_{self.alias_counter}"
        while any(name in s for s in reserved_names):
            self.alias_counter += 1
            name = f"{prefix}_{self.alias_counter}"
        self.alias_counter += 1
        return name

    def handle_collection_count(
        self,
        hybrid: HybridTree,
        expr: ExpressionFunctionCall,
        child_ref_mapping: dict[int, int],
    ) -> HybridExpr:
        """
        Special case of `make_hybrid_expr` specifically for expressions that
        are the COUNT of a subcollection.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of `expr`, as it is the context in which the `expr`
            will live.
            `expr`: the QDAG expression to be converted.
            `child_ref_mapping`: mapping of indices used by child references in
            the original expressions to the index of the child hybrid tree
            relative to the current level.

        Returns:
            The HybridExpr node corresponding to `expr`
        """
        assert expr.operator == pydop.COUNT, (
            f"Malformed call to handle_collection_count: {expr}"
        )
        assert len(expr.args) == 1, f"Malformed call to handle_collection_count: {expr}"
        collection_arg = expr.args[0]
        assert isinstance(collection_arg, ChildReferenceCollection), (
            f"Malformed call to handle_collection_count: {expr}"
        )
        count_call: HybridFunctionExpr = HybridFunctionExpr(
            pydop.COUNT, [], expr.pydough_type
        )
        child_idx: int = child_ref_mapping[collection_arg.child_idx]
        child_connection: HybridConnection = hybrid.children[child_idx]
        # Generate a unique name for the agg call to push into the child
        # connection. If the call already exists, reuse the existing name.
        agg_name: str
        if count_call in child_connection.aggs.values():
            agg_name = child_connection.fetch_agg_name(count_call)
        else:
            agg_name = self.gen_agg_name(child_connection)
            child_connection.aggs[agg_name] = count_call
        result_ref: HybridExpr = HybridChildRefExpr(
            agg_name, child_idx, expr.pydough_type
        )
        # The null-adding join is not done if this is the root level, since
        # that just means all the aggregations are no-groupby aggregations.
        joins_can_nullify: bool = not (
            isinstance(hybrid.pipeline[0], HybridRoot)
            or child_connection.connection_type.is_semi
        )
        return self.postprocess_agg_output(count_call, result_ref, joins_can_nullify)

    def convert_agg_arg(self, expr: HybridExpr, child_indices: set[int]) -> HybridExpr:
        """
        Translates a hybrid expression that is an argument to an aggregation
        (or a subexpression of such an argument) into a form that is expressed
        from the perspective of the child subtree that is being aggregated.

        Args:
            `expr`: the expression to be converted.
            `child_indices`: a set that is mutated to contain the indices of
            any children that are referenced by `expr`.

        Returns:
            The translated expression.

        Raises:
            NotImplementedError if `expr` is an expression that cannot be used
            inside of an aggregation call.
        """
        match expr:
            case HybridLiteralExpr():
                return expr
            case HybridChildRefExpr():
                # Child references become regular references because the
                # expression is phrased as if we were inside the child rather
                # than the parent.
                child_indices.add(expr.child_idx)
                return HybridRefExpr(expr.name, expr.typ)
            case HybridFunctionExpr():
                return HybridFunctionExpr(
                    expr.operator,
                    [self.convert_agg_arg(arg, child_indices) for arg in expr.args],
                    expr.typ,
                )
            case HybridBackRefExpr():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and fields of an ancestor of the current context"
                )
            case HybridRefExpr():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and fields of the context itself"
                )
            case HybridWindowExpr():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and window functions"
                )
            case _:
                raise NotImplementedError(
                    f"TODO: support converting {expr.__class__.__name__} in aggregations"
                )

    def make_agg_call(
        self,
        hybrid: HybridTree,
        expr: ExpressionFunctionCall,
        args: list[HybridExpr],
    ) -> HybridExpr:
        """
        For aggregate function calls, their arguments are translated in a
        manner that identifies what child subtree they correspond too, by
        index, and translates them relative to the subtree. Then, the
        aggregation calls are placed into the `aggs` mapping of the
        corresponding child connection, and the aggregation call becomes a
        child reference (referring to the aggs list), since after translation,
        an aggregated child subtree only has the grouping keys and the
        aggregation calls as opposed to its other terms.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of the aggregation call.
            `expr`: the aggregation function QDAG expression to be converted.
            `args`: the converted arguments to the aggregation call.
        """
        child_indices: set[int] = set()
        converted_args: list[HybridExpr] = [
            self.convert_agg_arg(arg, child_indices) for arg in args
        ]
        if len(child_indices) != 1:
            raise ValueError(
                f"Expected aggregation call to contain references to exactly one child collection, but found {len(child_indices)} in {expr}"
            )
        hybrid_call: HybridFunctionExpr = HybridFunctionExpr(
            expr.operator, converted_args, expr.pydough_type
        )
        # Identify the child connection that the aggregation call is pushed
        # into.
        child_idx: int = child_indices.pop()
        child_connection: HybridConnection = hybrid.children[child_idx]
        # If the aggregation already exists in the child, use a child reference
        # to it.
        agg_name: str
        if hybrid_call in child_connection.aggs.values():
            agg_name = child_connection.fetch_agg_name(hybrid_call)
        else:
            # Otherwise, Generate a unique name for the agg call to push into the
            # child connection.
            agg_name = self.gen_agg_name(child_connection)
            child_connection.aggs[agg_name] = hybrid_call
        result_ref: HybridExpr = HybridChildRefExpr(
            agg_name, child_idx, expr.pydough_type
        )
        joins_can_nullify: bool = not (
            isinstance(hybrid.pipeline[0], HybridRoot)
            or child_connection.connection_type.is_semi
        )
        return self.postprocess_agg_output(hybrid_call, result_ref, joins_can_nullify)

    def rewrite_median_call(
        self,
        child_connection: HybridConnection,
        expr: HybridFunctionExpr,
        create_new_calc: bool,
    ) -> HybridFunctionExpr:
        """
        Transforms a MEDIAN call into an AVG of the 1-2 median rows
        (obtained via window functions). This step must be done after
        de-correlation because it invokes the aggregation keys used for the
        child connection, which may change during de-correlation.

        Args:
            `child`: the child connection containing the aggregate call to
            MEDIAN as one of its aggs.
            `expr`: the aggregation function QDAG expression to be converted.
            `create_new_calc`: if True, creates a new CALCULATE when injecting
            the inputs to the AVG call into the child.
        """
        assert expr.operator == pydop.MEDIAN
        # Build an expression that makes all rows null except the 1-2 median
        # rows. The formula to find the kept rows is the following:
        #   ABS((r - 1) - (n - 1) / 2) < 1
        # Where `r` is the row number (sorted by the median column) and `n`
        # is the number of non-null rows of the median column. The window
        # functions are computed with the same partitioning keys that will be
        # used to aggregate the child connection.
        assert len(expr.args) == 1
        data_expr: HybridExpr = expr.args[0]
        one: HybridExpr = HybridLiteralExpr(Literal(1.0, NumericType()))
        two: HybridExpr = HybridLiteralExpr(Literal(2.0, NumericType()))
        assert child_connection.subtree.agg_keys is not None
        partition_args: list[HybridExpr] = child_connection.subtree.agg_keys
        order_args: list[HybridCollation] = [HybridCollation(data_expr, False, False)]
        rank: HybridExpr = HybridWindowExpr(
            pydop.RANKING, [], partition_args, order_args, NumericType(), {}
        )
        rows: HybridExpr = HybridWindowExpr(
            pydop.RELCOUNT, [data_expr], partition_args, [], NumericType(), {}
        )
        adjusted_rank: HybridExpr = HybridFunctionExpr(
            pydop.SUB, [rank, one], NumericType()
        )
        adjusted_rows: HybridExpr = HybridFunctionExpr(
            pydop.SUB, [rows, one], NumericType()
        )
        centerpoint: HybridExpr = HybridFunctionExpr(
            pydop.DIV, [adjusted_rows, two], NumericType()
        )
        distance_from_center = HybridFunctionExpr(
            pydop.ABS,
            [
                HybridFunctionExpr(
                    pydop.SUB, [adjusted_rank, centerpoint], NumericType()
                )
            ],
            NumericType(),
        )
        is_median_row: HybridExpr = HybridFunctionExpr(
            pydop.LET, [distance_from_center, one], BooleanType()
        )
        median_rows_arg: HybridExpr = HybridFunctionExpr(
            pydop.KEEP_IF, [data_expr, is_median_row], data_expr.typ
        )
        # Build a call to AVG on those 1-2 rows.
        median_rows_arg = self.inject_expression(
            child_connection.subtree, median_rows_arg, create_new_calc
        )
        avg_call: HybridFunctionExpr = HybridFunctionExpr(
            pydop.AVG, [median_rows_arg], expr.typ
        )

        return avg_call

    def rewrite_quantile_call(
        self,
        child_connection: HybridConnection,
        expr: HybridFunctionExpr,
        create_new_calc: bool,
    ) -> HybridFunctionExpr:
        """
        Rewrites a QUANTILE aggregation call into an equivalent expression using
        window functions. This is typically used for dialects that do not natively
        support the PERCENTILE_DISCaggregate function.

        The rewritten expression selects the value at the specified quantile by:
        - Ranking the rows within each partition.
        - Calculating the number of rows (N) in each partition.
        - Keeping only those rows where the rank is greater than
        INTEGER((1.0 - p) * N), where p is the quantile argument.
        - Taking the maximum value among the kept rows.

        Args:
            child_connection: The HybridConnection containing the aggregate call
            to QUANTILE.
            expr: The HybridFunctionExpr representing the QUANTILE aggregation.
            create_new_calc: If True, injects new expressions into a new CALCULATE
            operation.

        Returns:
            A HybridFunctionExpr representing the rewritten aggregation using
            window functions.
        """
        assert expr.operator == pydop.QUANTILE

        # Valid if the value of p is a number between 0 and 1
        if (
            not isinstance(expr.args[1], HybridLiteralExpr)
            or not isinstance(expr.args[1].typ, NumericType)
            or not isinstance(expr.args[1].literal.value, (int, float))
            or not (0.0 <= float(expr.args[1].literal.value) <= 1.0)
        ):
            raise PyDoughSQLException(
                f"Expected second argument to QUANTILE to be a numeric literal between 0 and 1, instead received {expr.args[1]!r}"
            )

        assert len(expr.args) == 2
        # The implementation
        # MAX(KEEP_IF(args[0], R > INTEGER((1.0-args[1]) * N)))
        data_expr: HybridExpr = expr.args[0]  # Column

        assert child_connection.subtree.agg_keys is not None
        partition_args: list[HybridExpr] = child_connection.subtree.agg_keys
        order_args: list[HybridCollation] = [HybridCollation(data_expr, False, False)]

        # R
        rank: HybridExpr = HybridWindowExpr(
            pydop.RANKING, [], partition_args, order_args, NumericType(), {}
        )
        # N
        rows: HybridExpr = HybridWindowExpr(
            pydop.RELCOUNT, [data_expr], partition_args, [], NumericType(), {}
        )

        # (1.0-args[1])
        sub: HybridExpr = HybridLiteralExpr(
            Literal(1.0 - float(expr.args[1].literal.value), NumericType())
        )

        # (1.0-args[1]) * N
        product: HybridExpr = HybridFunctionExpr(pydop.MUL, [sub, rows], NumericType())

        # INTEGER((1.0-args[1]) * N)
        cast_integer: HybridExpr = HybridFunctionExpr(
            pydop.INTEGER, [product], NumericType()
        )

        # R > INTEGER((1.0-args[1]) * N)
        greater: HybridExpr = HybridFunctionExpr(
            pydop.GRT, [rank, cast_integer], expr.typ
        )

        # KEEP_IF(args[0], R > INTEGER((1.0-args[1]) * N)))
        keep_largest: HybridExpr = HybridFunctionExpr(
            pydop.KEEP_IF, [data_expr, greater], data_expr.typ
        )

        # MAX
        max_input_arg = self.inject_expression(
            child_connection.subtree, keep_largest, create_new_calc
        )
        max_call: HybridFunctionExpr = HybridFunctionExpr(
            pydop.MAX, [max_input_arg], expr.typ
        )

        return max_call

    def add_unique_terms(
        self,
        hybrid: HybridTree,
        levels_remaining: int,
        levels_so_far: int,
        partition_args: list[HybridExpr],
        child_idx: int | None,
    ) -> None:
        """
        Populates a list of partition keys with the unique terms of an ancestor
        level of the hybrid tree.

        Args:
            `hybrid`: the hybrid tree whose ancestor's unique terms are being
            added to the partition keys.
            `levels_remaining`: the number of levels left to step back before
            the unique terms are added to the partition keys.
            `levels_so_far`: the number of levels that have been stepped back
            so far.
            `partition_args`: the list of partition keys that is being
            populated with the unique terms of the ancestor level.
            `child_idx`: the index to use when identifying that a child node
            has become correlated. If not provided, uses the value from the
            top of the stack.
        """
        # When the number of levels remaining to step back is 0, we have
        # reached the targeted ancestor, so we add the unique terms.
        if levels_remaining == 0:
            successor_join_mapping: dict[HybridExpr, HybridExpr] = {}
            if levels_so_far > 0 and hybrid.successor is not None:
                successor: HybridTree = hybrid.successor
                if isinstance(
                    successor.pipeline[0], HybridCollectionAccess
                ) and isinstance(successor.pipeline[0].collection, SubCollection):
                    sub_property: SubcollectionRelationshipMetadata = (
                        successor.pipeline[0].collection.subcollection_property
                    )
                    if isinstance(sub_property, SimpleJoinMetadata):
                        join_keys = HybridTranslator.get_subcollection_join_keys(
                            sub_property,
                            hybrid.pipeline[-1],
                            successor.pipeline[0],
                        )
                        for lhs, rhs in join_keys:
                            successor_join_mapping[lhs] = rhs
            for unique_term in sorted(hybrid.pipeline[-1].unique_exprs, key=str):
                if unique_term in successor_join_mapping:
                    partition_args.append(
                        successor_join_mapping[unique_term].shift_back(
                            levels_so_far - 1
                        )
                    )
                else:
                    partition_args.append(unique_term.shift_back(levels_so_far))
        elif hybrid.parent is None:
            # If we have not reached the target level yet, but we have reached
            # the top level of the tree, we need to step out of a child subtree
            # back into its parent and make a correlated reference.
            if len(self.stack) == 0:
                raise ValueError("Window function references too far back")
            prev_hybrid: HybridTree = self.stack.pop()
            correl_args: list[HybridExpr] = []
            self.add_unique_terms(
                prev_hybrid, levels_remaining - 1, 0, correl_args, child_idx
            )
            join_remapping: dict[HybridExpr, HybridExpr] = dict(
                hybrid.join_keys if hybrid.join_keys is not None else []
            )
            for arg in correl_args:
                if arg in join_remapping:
                    # Special case: if the uniqueness key is also a join key
                    # from the LHS, use the equivalent key from the RHS.
                    partition_args.append(join_remapping[arg].shift_back(levels_so_far))
                else:
                    # Otherwise, create a correlated reference to the term.
                    partition_args.append(HybridCorrelExpr(arg))
            self.stack.append(prev_hybrid)
        else:
            # Otherwise, we have to step back further, so we recursively
            # repeat the procedure one level further up in the hybrid tree.
            self.add_unique_terms(
                hybrid.parent,
                levels_remaining - 1,
                levels_so_far + 1,
                partition_args,
                child_idx,
            )

    def translate_back_reference(
        self, hybrid: HybridTree, expr: BackReferenceExpression
    ) -> HybridExpr:
        """
        Perform the logic used to translate a BACK reference in QDAG into a
        back reference in hybrid, or a correlated reference if the back
        reference steps back further than the height of the current hybrid
        tree.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of `expr`, as it is the context in which the `expr`
            will live.
            `expr`: the BACK reference to be converted.

        Returns:
            The HybridExpr node corresponding to `expr`.
        """
        back_levels: int = 0
        correl_levels: int = 0
        new_stack: list[HybridTree] = []
        ancestor_tree: HybridTree = hybrid
        expr_name: str = expr.term_name
        # Start with the current context and hunt for an ancestor with
        # that name pinned by a CALCULATE (so it is in the ancestral
        # mapping), and make sure it is within the height bounds of the
        # current tree. If not, then pop the previous tree from the
        # stack and look there, repeating until one is found or the
        # stack is exhausted. Keep track of how many times we step
        # outward, since this is how many CORREL() layers we need to
        # wrap the final expression in.
        while True:
            if (
                expr.term_name in ancestor_tree.ancestral_mapping
                and ancestor_tree.ancestral_mapping[expr.term_name]
                < ancestor_tree.get_tree_height()
            ):
                back_levels = ancestor_tree.ancestral_mapping[expr.term_name]
                for _ in range(back_levels):
                    assert ancestor_tree.parent is not None
                    ancestor_tree = ancestor_tree.parent
                expr_name = ancestor_tree.pipeline[-1].renamings.get(
                    expr_name, expr_name
                )
                break
            elif len(self.stack) > 0:
                ancestor_tree = self.stack.pop()
                new_stack.append(ancestor_tree)
                correl_levels += 1
            else:
                raise ValueError("Cannot find ancestor with name " + str(expr))
        for tree in reversed(new_stack):
            self.stack.append(tree)

        # The final expression is a regular or back reference depending
        # on how many back levels it is from the identified ancestor.
        result: HybridExpr
        if back_levels == 0:
            result = HybridRefExpr(expr_name, expr.pydough_type)
        else:
            result = HybridBackRefExpr(expr_name, back_levels, expr.pydough_type)

        # Then, wrap it in the necessary number of CORREL() layers.
        for _ in range(correl_levels):
            result = HybridCorrelExpr(result)

        return result

    def make_hybrid_expr(
        self,
        hybrid: HybridTree,
        expr: PyDoughExpressionQDAG,
        child_ref_mapping: dict[int, int],
        inside_agg: bool,
    ) -> HybridExpr:
        """
        Converts a QDAG expression into a HybridExpr.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of `expr`, as it is the context in which the `expr`
            will live.
            `expr`: the QDAG expression to be converted.
            `child_ref_mapping`: mapping of indices used by child references in
            the original expressions to the index of the child hybrid tree
            relative to the current level.
            `inside_agg`: True if `expr` is being derived is inside of an
            aggregation call, False otherwise.

        Returns:
            The HybridExpr node corresponding to `expr`
        """
        expr_name: str
        child_connection: HybridConnection
        args: list[HybridExpr] = []
        hybrid_arg: HybridExpr
        collection: PyDoughCollectionQDAG
        match expr:
            case PartitionKey():
                return self.make_hybrid_expr(
                    hybrid, expr.expr, child_ref_mapping, inside_agg
                )
            case Literal():
                return HybridLiteralExpr(expr)
            case ColumnProperty():
                return HybridColumnExpr(expr)
            case ChildReferenceExpression():
                # A reference to an expression from a child subcollection
                # becomes a reference to one of the terms of one of the child
                # subtrees of the current hybrid tree.
                hybrid_child_index: int = child_ref_mapping[expr.child_idx]
                child_connection = hybrid.children[hybrid_child_index]
                expr_name = child_connection.subtree.pipeline[-1].renamings.get(
                    expr.term_name, expr.term_name
                )
                return HybridChildRefExpr(
                    expr_name, hybrid_child_index, expr.pydough_type
                )
            case SidedReference():
                if expr.is_parent:
                    return HybridSidedRefExpr(
                        HybridRefExpr(expr.term_name, expr.pydough_type)
                    )
                else:
                    return HybridRefExpr(expr.term_name, expr.pydough_type)
            case BackReferenceExpression():
                return self.translate_back_reference(hybrid, expr)

            case Reference():
                if hybrid.ancestral_mapping.get(expr.term_name, 0) > 0:
                    collection = expr.collection
                    while (
                        isinstance(collection, PartitionChild)
                        and expr.term_name in collection.child_access.ancestral_mapping
                    ):
                        collection = collection.child_access
                    return self.make_hybrid_expr(
                        hybrid,
                        BackReferenceExpression(
                            collection,
                            expr.term_name,
                            hybrid.ancestral_mapping[expr.term_name],
                        ),
                        child_ref_mapping,
                        inside_agg,
                    )
                expr_name = hybrid.pipeline[-1].renamings.get(
                    expr.term_name, expr.term_name
                )
                return HybridRefExpr(expr_name, expr.pydough_type)
            case ExpressionFunctionCall():
                if expr.operator.is_aggregation and inside_agg:
                    raise NotImplementedError(
                        f"PyDough does not yet support calling aggregations inside of aggregations: {expr!r}"
                    )
                # Do special casing for operators that can have collection
                # arguments.
                # TODO: (gh #148) handle collection-level NDISTINCT
                if (
                    expr.operator == pydop.COUNT
                    and len(expr.args) == 1
                    and isinstance(expr.args[0], PyDoughCollectionQDAG)
                ):
                    return self.handle_collection_count(hybrid, expr, child_ref_mapping)
                elif expr.operator in (pydop.HAS, pydop.HASNOT):
                    # Since the connection has been mutated to be a semi/anti join, the
                    # has / hasnot condition is now known to be true.
                    return HybridLiteralExpr(Literal(True, BooleanType()))

                # For normal operators, translate their expression arguments
                # normally. If it is a non-aggregation, build the function
                # call. If it is an aggregation, transform accordingly.
                # such function that takes in a collection, as none currently
                # exist that are not aggregations.
                for arg in expr.args:
                    if not isinstance(arg, PyDoughExpressionQDAG):
                        raise NotImplementedError(
                            f"Non-expression argument {arg!r} of type {arg.__class__.__name__} found in operator {expr.operator.function_name!r}"
                        )
                    args.append(
                        self.make_hybrid_expr(
                            hybrid,
                            arg,
                            child_ref_mapping,
                            inside_agg or expr.operator.is_aggregation,
                        )
                    )
                if expr.operator.is_aggregation:
                    return self.make_agg_call(hybrid, expr, args)
                else:
                    return HybridFunctionExpr(expr.operator, args, expr.pydough_type)
            case WindowCall():
                partition_args: list[HybridExpr] = []
                order_args: list[HybridCollation] = []
                # If the levels argument was provided, find the partition keys
                # for that ancestor level.
                if expr.levels is not None:
                    self.add_unique_terms(hybrid, expr.levels, 0, partition_args, None)
                # Convert all of the window function arguments to hybrid
                # expressions.
                for arg in expr.args:
                    args.append(
                        self.make_hybrid_expr(
                            hybrid, arg, child_ref_mapping, inside_agg
                        )
                    )
                # Convert all of the ordering terms to hybrid expressions.
                for col_arg in expr.collation_args:
                    hybrid_arg = self.make_hybrid_expr(
                        hybrid, col_arg.expr, child_ref_mapping, inside_agg
                    )
                    order_args.append(
                        HybridCollation(hybrid_arg, col_arg.asc, col_arg.na_last)
                    )
                # Build the new hybrid window function call with all the
                # converted terms.
                return HybridWindowExpr(
                    expr.window_operator,
                    args,
                    partition_args,
                    order_args,
                    expr.pydough_type,
                    expr.kwargs,
                )
            case _:
                raise NotImplementedError(
                    f"TODO: support converting {expr.__class__.__name__}"
                )

    def process_hybrid_collations(
        self,
        hybrid: HybridTree,
        collations: list[CollationExpression],
        child_ref_mapping: dict[int, int],
    ) -> tuple[dict[str, HybridExpr], list[HybridCollation]]:
        """
        Converts a list of CollationExpression objects into a dictionary of
        new expressions for generating a `CALCULATE` and a list of
        HybridCollation values.

        Args:
            `hybrid` The hybrid tree used to handle ordering expressions.
            `collations` The collations to process and convert to
                HybridCollation values.
            `child_ref_mapping` The child mapping to track for handling
                child references in the collations.

        Returns:
            A tuple containing a dictionary of new expressions for generating
            a `CALCULATE` and a list of the new HybridCollation values.
        """
        new_expressions: dict[str, HybridExpr] = {}
        hybrid_orderings: list[HybridCollation] = []
        name: str
        expr: HybridExpr
        for collation in collations:
            if type(collation.expr) is Reference:
                name = collation.expr.term_name
            else:
                name = self.get_ordering_name(hybrid)
                expr = self.make_hybrid_expr(
                    hybrid, collation.expr, child_ref_mapping, False
                )
                new_expressions[name] = expr
            new_collation: HybridCollation = HybridCollation(
                HybridRefExpr(name, collation.expr.pydough_type),
                collation.asc,
                not collation.na_last,
            )
            hybrid_orderings.append(new_collation)
        return new_expressions, hybrid_orderings

    def define_root_link(
        self, parent: HybridTree, tree: HybridTree, is_agg: bool
    ) -> None:
        """
        Extracts the information required to link a parent hybrid tree to a
        child hybrid tree, and stores it within the child tree. There are three
        kinds of information that can be extracted
        - `join_keys`: a list of tuples of join keys, where each tuple is
        in the form `(lhs_key, rhs_key)` where `lhs_key` is an expression
        from `parent`, `rhs_key` is an expression from `tree`, and the
        condition `lhs_key == rhs_key` must be true when joining parent to
        tree. This is `None` if the connection between `parent` and
        `tree` is not an equi-join.
        - `agg_keys`: a list of expressions from `tree` that should be used to
        aggregate the child tree before joining it onto the parent tree, if
        aggregation is required. This is `None` if there is no aggregation.
        - `general_join_condition`: a hybrid expression used as a filter to
        decide when records of `parent` and `tree` should be joined.
        Expressions from the `parent` are wrapped in a HybridSidedRefExpr,
        while expressions from the child are normal references. This is
        `None` if the connection between `parent` and `tree` is not a general
        join.

        Args:
            `parent`: the parent hybrid tree that `tree` is a child of.
            `tree`: the hybrid tree whose connection to `parent` is being
            analyzed.
            `is_agg`: True if the connection is being analyzed in the context
            of an aggregation, False otherwise.
        """
        join_keys: list[tuple[HybridExpr, HybridExpr]] | None = None
        agg_keys: list[HybridExpr] | None = None
        general_join_cond: HybridExpr | None = None
        operation: HybridOperation = tree.pipeline[0]
        match operation:
            case HybridCollectionAccess():
                if isinstance(operation.collection, TableCollection):
                    # A table collection does not need to be joined onto its
                    # parent.
                    join_keys = []
                else:
                    # A sub-collection needs to be joined onto its parent using
                    # the join protocol defined by its metadata.
                    assert isinstance(operation.collection, SubCollection)
                    sub_property: SubcollectionRelationshipMetadata = (
                        operation.collection.subcollection_property
                    )
                    if isinstance(sub_property, SimpleJoinMetadata):
                        # For a simple join access, populate the join
                        # keys of the tree which will be bubbled
                        # down throughout the entire child tree.
                        assert parent is not None
                        join_keys = HybridTranslator.get_subcollection_join_keys(
                            sub_property,
                            parent.pipeline[-1],
                            operation,
                        )
                    elif isinstance(sub_property, GeneralJoinMetadata):
                        # For general join sub-collection accesses, use the
                        # general join property already stored in the operation.
                        general_join_cond = operation.general_condition
                    else:
                        # For cartesian product accesses, there is no need for
                        # join keys since it will be a cross join.
                        join_keys = []
            case HybridPartitionChild():
                # A partition child is joined onto its parent using the
                # aggregation keys as join keys.
                assert operation.subtree.agg_keys is not None
                join_keys = []
                for key in operation.subtree.agg_keys:
                    join_keys.append((key, key))

            case HybridPartition():
                # A partition does not need to be joined to its parent
                join_keys = []
            case HybridRoot():
                # A root does not need to be joined to its parent
                join_keys = []
            case _:
                raise NotImplementedError(f"{operation.__class__.__name__}")
        if join_keys is not None:
            # For a simple join, add the join keys to the child
            # and make the RHS of those keys the agg keys.
            agg_keys = [rhs_key for _, rhs_key in join_keys]
        else:
            # If this is a general join with aggregation, use the general join condition
            # condition. However, if an aggregate is being performed
            # on the child, the aggregation keys are the uniqueness
            # keys from the lhs of the join condition, which means
            # a calculate must be added to the child that accesses
            # those keys as correlated references, then uses those
            # terms from the calculate in the agg keys.
            if is_agg:
                lhs_unique_keys: dict[str, HybridExpr] = {}
                agg_keys = []
                back_levels: int = 0
                current_level: HybridTree | None = parent
                # First, find the uniqueness keys from every level of
                # the parent and add them to lhs_unique_keys as a
                # correlated reference.
                while current_level is not None:
                    for expr in current_level.pipeline[-1].unique_exprs:
                        key_name = f"key_{len(lhs_unique_keys)}"
                        expr = HybridCorrelExpr(expr.shift_back(back_levels))
                        lhs_unique_keys[key_name] = expr
                    back_levels += 1
                    current_level = current_level.parent
                # Insert the calculate to access these correlated
                # keys, then add references to the new terms to
                # the agg keys.
                new_calc: HybridOperation = HybridCalculate(
                    tree.pipeline[-1], lhs_unique_keys, []
                )
                new_calc.is_hidden = True
                tree.add_operation(new_calc)
                for key_name, expr in lhs_unique_keys.items():
                    agg_keys.append(HybridRefExpr(key_name, expr.typ))

        # Set the join keys, aggregation keys, and general join condition of
        # the child tree to the defined values.
        tree._join_keys = join_keys
        tree._agg_keys = agg_keys
        tree._general_join_condition = general_join_cond

    def make_hybrid_tree(
        self,
        node: PyDoughCollectionQDAG,
        parent: HybridTree | None,
        is_aggregate: bool = False,
    ) -> HybridTree:
        """
        Converts a collection QDAG into the HybridTree format.

        Args:
            `node`: the collection QDAG to be converted.
            `parent`: optional hybrid tree of the parent context that `node` is
            a child of.
            `is_aggregate`: True if the node is being aggregated with regards
            to `parent`, False otherwise.

        Returns:
            The HybridTree representation of `node`.
        """
        hybrid: HybridTree
        subtree: HybridTree
        successor_hybrid: HybridTree
        expr: HybridExpr
        child_ref_mapping: dict[int, int] = {}
        key_exprs: list[HybridExpr] = []
        collection_access: HybridCollectionAccess
        match node:
            case GlobalContext():
                if node.ancestor_context is None:
                    # No ancestor context, so this is the root of the hybrid tree.
                    return HybridTree(HybridRoot(), node.ancestral_mapping)
                else:
                    # For CROSS operations, need to create a hybrid tree for the
                    # ancestor context, which is the context of the CROSS.
                    hybrid = self.make_hybrid_tree(
                        node.ancestor_context, parent, is_aggregate
                    )
                    # Create a new hybrid tree for the current global context, which
                    # will be the a successor of the hybrid tree for the ancestor context.
                    successor_hybrid = HybridTree(HybridRoot(), node.ancestral_mapping)
                    hybrid.add_successor(successor_hybrid)
                    return successor_hybrid
            case TableCollection() | SubCollection():
                collection_access = HybridCollectionAccess(node)
                successor_hybrid = HybridTree(collection_access, node.ancestral_mapping)
                # If accessing a sub-collection with a general join condition,
                # populate the general_condition field of the sub-collection
                # access with the general join condition converted from a QDAG
                # expression to a hybrid expression.
                if isinstance(node, SubCollection) and isinstance(
                    node.subcollection_property, GeneralJoinMetadata
                ):
                    assert node.general_condition is not None
                    collection_access.general_condition = self.make_hybrid_expr(
                        successor_hybrid,
                        node.general_condition,
                        {},
                        False,
                    )
                hybrid = self.make_hybrid_tree(
                    node.ancestor_context, parent, is_aggregate
                )
                hybrid.add_successor(successor_hybrid)
                return successor_hybrid
            case PartitionChild():
                hybrid = self.make_hybrid_tree(
                    node.ancestor_context, parent, is_aggregate
                )
                # Identify the original data being partitioned, which may
                # require stepping in multiple times if the partition is
                # nested inside another partition.
                src_tree: HybridTree = hybrid
                while isinstance(src_tree.pipeline[0], HybridPartitionChild):
                    src_tree = src_tree.pipeline[0].subtree
                subtree = src_tree.children[0].subtree
                successor_hybrid = HybridTree(
                    HybridPartitionChild(subtree),
                    node.ancestral_mapping,
                )
                hybrid.add_successor(successor_hybrid)
                return successor_hybrid
            case Calculate():
                hybrid = self.make_hybrid_tree(
                    node.preceding_context, parent, is_aggregate
                )
                self.populate_children(hybrid, node, child_ref_mapping)
                new_expressions: dict[str, HybridExpr] = {}
                for name in sorted(node.calc_terms):
                    expr = self.make_hybrid_expr(
                        hybrid, node.get_expr(name), child_ref_mapping, False
                    )
                    new_expressions[name] = expr
                hybrid.add_operation(
                    HybridCalculate(
                        hybrid.pipeline[-1],
                        new_expressions,
                        hybrid.pipeline[-1].orderings,
                    )
                )
                for name in new_expressions:
                    hybrid.ancestral_mapping[name] = 0
                return hybrid
            case Singular():
                # a Singular node is just used to annotate the preceding context
                # with additional information with respect to parent context.
                # This information is no longer needed (as it has been used in
                # conversion from Unqualified to QDAG), so it can be discarded
                # and replaced with the preceding context.
                hybrid = self.make_hybrid_tree(
                    node.preceding_context, parent, is_aggregate
                )
                return hybrid
            case Where():
                hybrid = self.make_hybrid_tree(
                    node.preceding_context, parent, is_aggregate
                )
                old_length: int = len(hybrid.pipeline)
                self.populate_children(hybrid, node, child_ref_mapping)
                expr = self.make_hybrid_expr(
                    hybrid, node.condition, child_ref_mapping, False
                )
                # Special case: if the act of calling populate_children created
                # more filters, insert the filter before the new filters.
                if old_length < len(hybrid.pipeline):
                    new_operations: int = len(hybrid.pipeline) - old_length
                    prev_op: HybridOperation = hybrid.pipeline[-new_operations - 1]
                    new_filter = HybridFilter(prev_op, expr)
                    next_op: HybridOperation = hybrid.pipeline[-new_operations]
                    assert isinstance(next_op, HybridFilter)
                    next_op.predecessor = new_filter
                    hybrid.pipeline.insert(-new_operations, new_filter)
                else:
                    hybrid.add_operation(HybridFilter(hybrid.pipeline[-1], expr))
                return hybrid
            case PartitionBy():
                hybrid = self.make_hybrid_tree(
                    node.ancestor_context, parent, is_aggregate
                )
                partition: HybridPartition = HybridPartition()
                successor_hybrid = HybridTree(partition, node.ancestral_mapping)
                hybrid.add_successor(successor_hybrid)
                self.populate_children(successor_hybrid, node, child_ref_mapping)
                partition_child_idx: int = child_ref_mapping[0]
                for key_name in sorted(node.calc_terms, key=str):
                    key = node.get_expr(key_name)
                    expr = self.make_hybrid_expr(
                        successor_hybrid, key, child_ref_mapping, False
                    )
                    partition.add_key(key_name, expr)
                    key_exprs.append(HybridRefExpr(key_name, expr.typ))
                partition_child: HybridTree = successor_hybrid.children[
                    partition_child_idx
                ].subtree
                partition_child.agg_keys = key_exprs
                partition_child.join_keys = [(k, k) for k in key_exprs]
                # Add a dummy no-op after the partition to ensure a
                # buffer in the max_steps for other children.
                successor_hybrid.add_operation(
                    HybridNoop(successor_hybrid.pipeline[-1])
                )
                return successor_hybrid
            case OrderBy() | TopK():
                hybrid = self.make_hybrid_tree(
                    node.preceding_context, parent, is_aggregate
                )
                self.populate_children(hybrid, node, child_ref_mapping)
                new_nodes: dict[str, HybridExpr]
                hybrid_orderings: list[HybridCollation]
                new_nodes, hybrid_orderings = self.process_hybrid_collations(
                    hybrid, node.collation, child_ref_mapping
                )
                hybrid.add_operation(
                    HybridCalculate(hybrid.pipeline[-1], new_nodes, hybrid_orderings)
                )
                if isinstance(node, TopK):
                    hybrid.add_operation(
                        HybridLimit(hybrid.pipeline[-1], node.records_to_keep)
                    )
                return hybrid
            case ChildOperatorChildAccess():
                assert parent is not None
                match node.child_access:
                    case TableCollection() | SubCollection():
                        collection_access = HybridCollectionAccess(node.child_access)
                        successor_hybrid = HybridTree(
                            collection_access,
                            node.ancestral_mapping,
                        )
                        if isinstance(node.child_access, SubCollection):
                            sub_property: SubcollectionRelationshipMetadata = (
                                node.child_access.subcollection_property
                            )
                            if isinstance(sub_property, GeneralJoinMetadata):
                                # For general join, do the same except with the
                                # general condition instead of equi-join keys.
                                assert node.child_access.general_condition is not None
                                collection_access.general_condition = (
                                    self.make_hybrid_expr(
                                        successor_hybrid,
                                        node.child_access.general_condition,
                                        {},
                                        False,
                                    )
                                )
                            elif not isinstance(
                                sub_property,
                                (SimpleJoinMetadata, CartesianProductMetadata),
                            ):
                                raise NotImplementedError(
                                    f"Unsupported metadata type for subcollection access: {sub_property.__class__.__name__}"
                                )
                    case PartitionChild():
                        source: HybridTree = parent
                        if isinstance(source.pipeline[0], HybridPartitionChild):
                            source = source.pipeline[0].subtree
                        successor_hybrid = HybridTree(
                            HybridPartitionChild(source.children[0].subtree),
                            node.ancestral_mapping,
                        )
                    case PartitionBy():
                        partition = HybridPartition()
                        successor_hybrid = HybridTree(partition, node.ancestral_mapping)
                        self.populate_children(
                            successor_hybrid, node.child_access, child_ref_mapping
                        )
                        partition_child_idx = child_ref_mapping[0]
                        successor_hybrid.children[
                            partition_child_idx
                        ].subtree.squish_backrefs_into_correl(None, 1)
                        for key_name in node.calc_terms:
                            key = node.get_expr(key_name)
                            expr = self.make_hybrid_expr(
                                successor_hybrid, key, child_ref_mapping, False
                            )
                            partition.add_key(key_name, expr)
                            key_exprs.append(HybridRefExpr(key_name, expr.typ))
                        successor_hybrid.children[
                            partition_child_idx
                        ].subtree.agg_keys = key_exprs
                        # Add a dummy no-op after the partition to ensure a
                        # buffer in the max_steps for other children.
                        successor_hybrid.add_operation(
                            HybridNoop(successor_hybrid.pipeline[-1])
                        )
                    case GlobalContext():
                        # This is a special case where the child access
                        # is a global context, which means that the child is
                        # a separate top-level computation (hybrid tree).
                        successor_hybrid = HybridTree(
                            HybridRoot(), node.ancestral_mapping
                        )
                    case _:
                        raise NotImplementedError(
                            f"{node.__class__.__name__} (child is {node.child_access.__class__.__name__})"
                        )
                self.define_root_link(parent, successor_hybrid, is_aggregate)
                return successor_hybrid
            case _:
                raise NotImplementedError(f"{node.__class__.__name__}")

    def run_syncretization(self, hybrid: "HybridTree") -> None:
        """
        Invokes the procedure to syncretize the children in the hybrid tree.
        The transformation is done in-place.

        Args:
            `hybrid`: The hybrid tree to run syncretization on.
        """
        sync: HybridSyncretizer = HybridSyncretizer(self)
        return sync.syncretize_children(hybrid)

    def run_correlation_extraction(self, hybrid: "HybridTree") -> None:
        """
        Invokes the procedure to extract correlated references from the hybrid
        tree, attempting to coerce some filters with correlated references into
        join conditions. The transformation is done in-place.

        Args:
            `hybrid`: The hybrid tree to run correlation extraction on.
        """
        extractor: HybridCorrelationExtractor = HybridCorrelationExtractor(self)
        extractor.run_correlation_extraction(hybrid)

    def run_hybrid_decorrelation(self, hybrid: "HybridTree") -> None:
        """
        Invokes the procedure to remove correlated references from a hybrid tree
        before relational conversion if those correlated references are invalid
        (e.g. not from a semi/anti join). The transformation is done in-place.

        Args:
            `hybrid`: The hybrid tree to remove correlated references from.
        """
        decorr: HybridDecorrelater = HybridDecorrelater()
        decorr.find_correlated_children(hybrid)
        decorr.decorrelate_hybrid_tree(hybrid)

    def convert_qdag_to_hybrid(self, node: PyDoughCollectionQDAG) -> HybridTree:
        """
        Convert a PyDough QDAG node to a hybrid tree, including any necessary
        transformations such as de-correlation.

        Args:
            `node`: The PyDoughCollectionQDAG node to convert to a HybridTree.

        Returns:
            The HybridTree representation of the given QDAG node after
            transformations.
        """
        # 1. Run the initial conversion from QDAG to Hybrid
        hybrid: HybridTree = self.make_hybrid_tree(node, None)
        # 2. Eject any aggregate inputs from the hybrid tree.
        self.eject_aggregate_inputs(hybrid)
        # 3. Syncretize any children of the hybrid tree that share a common
        # prefix, thus eliminating duplicate logic.
        self.run_syncretization(hybrid)
        # 4. Run the correlation extraction procedure to attempt to coerce some
        # filters with correlated references into join conditions.
        self.run_correlation_extraction(hybrid)
        # 5. Run the de-correlation procedure.
        self.run_hybrid_decorrelation(hybrid)
        # 6. Run any final rewrites, such as turning MEDIAN into an average
        # of the 1-2 median rows, that must happen after de-correlation.
        self.run_rewrites(hybrid)
        # 7. Remove any dead children in the hybrid tree that are no longer
        # being used.
        hybrid.remove_dead_children(set())
        return hybrid
