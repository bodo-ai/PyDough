"""
Logic for detecting mask/unmask calls within the final relational plan that will
cause a critical logical error if the user does not have permission to make the
call, and logging warnings for those calls.
"""

__all__ = ["MaskingCriticalDetectionVisitor"]

import pydough.pydough_operators as pydop
from pydough.logger import get_logger
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    CorrelatedReference,
    EmptySingleton,
    Filter,
    GeneratedTable,
    Join,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalExpressionVisitor,
    RelationalNode,
    RelationalRoot,
    RelationalVisitor,
    Scan,
    WindowCallExpression,
)
from pydough.relational.rel_util import add_input_name


class MaskingCriticalDetectionExpressionVisitor(RelationalExpressionVisitor):
    """
    A visitor to detect mask/unmask calls within expressions based on which
    columns from the input relational node depend on mask/unmask calls. After
    calling accept with an expression, the stack will contain a singleton list
    with a set of (column_name, is_unmask) tuples representing the table
    columns that the expression depends on via mask/unmask calls, and whether
    they depend on masking or unmasking.
    """

    def __init__(self) -> None:
        self.input_dependencies: dict[RelationalExpression, set[tuple[str, bool]]] = {}
        self.stack: list[set[tuple[str, bool]]] = []

    def reset(self) -> None:
        self.stack = []

    def visit_call_expression(self, expr: CallExpression) -> None:
        # Aggregate the dependencies from all input expressions
        dependencies: set[tuple[str, bool]] = set()
        for input_expr in expr.inputs:
            input_expr.accept(self)
            dependencies.update(self.stack.pop())
        # If this call expression is a mask/unmask operation, add the relevant
        # column dependency.
        if isinstance(expr.op, pydop.MaskedExpressionFunctionOperator):
            dependencies.add(
                (
                    f"{expr.op.table_path}.{expr.op.masking_metadata.column_name}",
                    expr.op.is_unmask,
                )
            )
        self.stack.append(dependencies)

    def visit_window_expression(self, expr: WindowCallExpression) -> None:
        # Aggregate the dependencies from all input, partition, and order
        # expressions.
        dependencies: set[tuple[str, bool]] = set()
        for input_expr in expr.inputs:
            input_expr.accept(self)
            dependencies.update(self.stack.pop())
        for partition_expr in expr.partition_inputs:
            partition_expr.accept(self)
            dependencies.update(self.stack.pop())
        for order_expr in expr.order_inputs:
            order_expr.expr.accept(self)
            dependencies.update(self.stack.pop())
        self.stack.append(dependencies)

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        # Retrieve the dependencies for this column from the input dependencies.
        self.stack.append(self.input_dependencies.get(column_reference, set()))

    def visit_correlated_reference(
        self, correlated_reference: CorrelatedReference
    ) -> None:
        # Correlated references have no dependencies on masking/unmasking.
        self.stack.append(set())

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        # Literal expressions have no dependencies on masking/unmasking.
        self.stack.append(set())


class MaskingCriticalDetectionVisitor(RelationalVisitor):
    """
    The main visitor which traverses the relational tree, inferring which
    columns depending on mask/unmask calls, propagating them upward through
    the plan, and logging warnings for any mask/unmask calls that are
    critical to the output of the query.
    """

    def __init__(self) -> None:
        self.critical_mask_columns: set[str] = set()
        """
        The set of fully qualified column names where a MASK operation on the
        column is critical to the output of the query.
        """

        self.critical_unmask_columns: set[str] = set()

        """
        The set of fully qualified column names where an UNMASK operation on the
        column is critical to the output of the query.
        """

        self.expression_visitor = MaskingCriticalDetectionExpressionVisitor()
        """
        The expression visitor used to detect mask/unmask dependencies within
        expressions.
        """

        self.stack: list[dict[RelationalExpression, set[tuple[str, bool]]]] = []
        """
        The stack of input dependency mappings for each relational node visited.
        Each mapping corresponds to a relational node from one of the inputs to
        the current node, and maps each output expression of the node to the set
        of (column_name, is_unmask) tuples that the expression depends on.
        """

    def reset(self) -> None:
        self.critical_mask_columns.clear()
        self.critical_unmask_columns.clear()
        self.stack.clear()
        self.expression_visitor.reset()

    def visit_inputs(self, node: RelationalNode) -> None:
        """
        Generic logic to visit all input nodes of a relational node, and build
        the input dependency mapping for the current node that will be given to
        the expression visitor so it knows which column references have
        dependencies on mask/unmask calls.
        """
        input_dependencies: dict[RelationalExpression, set[tuple[str, bool]]] = {}

        # Loop over all of the input nodes and recursively visit them,
        # extracting their dependencies from the stack.
        for idx, input_node in enumerate(node.inputs):
            input_node.accept(self)
            dependencies: dict[RelationalExpression, set[tuple[str, bool]]] = (
                self.stack.pop()
            )
            # If the node has 1 input, then its dependencies are what should
            # be used.
            if len(node.inputs) == 1:
                input_dependencies = dependencies

            # Otherwise, we need to map the dependencies to the appropriate
            # input alias for the node.
            else:
                alias: str | None = node.default_input_aliases[idx]
                for expr, deps in dependencies.items():
                    input_dependencies[add_input_name(expr, alias)] = deps

        # Register the unmask/mask call dependencies from all inputs to this
        # node with the expression visitor.
        self.expression_visitor.input_dependencies = input_dependencies

    def find_critical_dependencies(self, expr: RelationalExpression) -> None:
        """
        Takes in an expression used in a critical manner (join condition,
        filter condition, aggregate key, or ordering key for a root/limit), and
        feeds it to the expression visitor to determine if it has any mask/unmask
        call dependencies. If it does, the relevant columns are added to the
        critical mask/unmask column sets.

        Args:
            `expr`: The expression to analyze for any mask/unmask dependencies.
        """
        expr.accept(self.expression_visitor)
        expr_dependencies: set[tuple[str, bool]] = self.expression_visitor.stack.pop()
        for col_name, is_unmask in expr_dependencies:
            if is_unmask:
                self.critical_unmask_columns.add(col_name)
            else:
                self.critical_mask_columns.add(col_name)

    def add_output_dependencies(self, node: RelationalNode) -> None:
        """
        Uses the expression visitor to determine the mask/unmask dependencies
        for each output expression of the given relational node, and pushes the
        resulting mapping onto the stack.

        Args:
            `node`: The relational node whose output columns are having their
            dependencies determined.
        """
        out_dependencies: dict[RelationalExpression, set[tuple[str, bool]]] = {}
        for name, expr in node.columns.items():
            expr.accept(self.expression_visitor)
            out_dependencies[ColumnReference(name, expr.data_type)] = (
                self.expression_visitor.stack.pop()
            )
        self.stack.append(out_dependencies)

    def log_critical_calls(self) -> None:
        """
        Logs warnings for all critical mask/unmask calls detected during the
        traversal of the tree.

        This should be called once after the visitor has traversed the entire
        relational plan.
        """
        logger = get_logger()
        for column in self.critical_mask_columns:
            logger.warning(
                f"Query will not produce a valid output unless user has permission to mask column `{column}`"
            )
        for column in self.critical_unmask_columns:
            logger.warning(
                f"Query will not produce a valid output unless user has permission to unmask column `{column}`"
            )

        # Clean up the visitor afterwards, to avoid accidentally logging a
        # duplicate.
        self.reset()

    def visit_project(self, project: Project) -> None:
        # Projects simply propagate dependencies from their inputs.
        self.visit_inputs(project)
        self.add_output_dependencies(project)

    def visit_filter(self, filter: Filter) -> None:
        # Filter nodes propagate dependencies from their inputs, but also
        # analyze their condition for critical dependencies.
        self.visit_inputs(filter)
        self.find_critical_dependencies(filter.condition)
        self.add_output_dependencies(filter)

    def visit_join(self, join: Join) -> None:
        # Filter nodes propagate dependencies from their inputs, but also
        # analyze their condition for critical dependencies.
        self.visit_inputs(join)
        self.find_critical_dependencies(join.condition)
        self.add_output_dependencies(join)

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        # Aggregate nodes propagate dependencies from their inputs, but also
        # analyze their aggregation keys for critical dependencies.
        self.visit_inputs(aggregate)
        for agg_key in aggregate.keys.values():
            self.find_critical_dependencies(agg_key)
        self.add_output_dependencies(aggregate)

    def visit_limit(self, limit: Limit) -> None:
        # Limit nodes propagate dependencies from their inputs, but also
        # analyze their ordering keys for critical dependencies.
        self.visit_inputs(limit)
        for order_expr in limit.orderings:
            self.find_critical_dependencies(order_expr.expr)
        self.add_output_dependencies(limit)

    def visit_root(self, root: RelationalRoot) -> None:
        # Root nodes propagate dependencies from their inputs, but also
        # analyze their ordering keys for critical dependencies.
        self.visit_inputs(root)
        for order_expr in root.orderings:
            self.find_critical_dependencies(order_expr.expr)
        self.add_output_dependencies(root)

    def visit_scan(self, scan: Scan) -> None:
        # Scan nodes have no inputs, so they propagate dependencies based on
        # their columns relative to an empty input.
        self.expression_visitor.input_dependencies = {}
        self.add_output_dependencies(scan)

    def visit_generated_table(self, generated_table: GeneratedTable) -> None:
        # GeneratedTable nodes have no inputs, so they propagate dependencies based on
        # their columns relative to an empty input.
        self.expression_visitor.input_dependencies = {}
        self.add_output_dependencies(generated_table)

    def visit_empty_singleton(self, empty_singleton: EmptySingleton) -> None:
        # Empty singletons have no inputs, so they propagate dependencies based
        # on their columns relative to an empty input.
        self.expression_visitor.input_dependencies = {}
        self.add_output_dependencies(empty_singleton)
