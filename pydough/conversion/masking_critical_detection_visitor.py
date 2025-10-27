"""
TODO
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
    A visitor to XXX.
    """

    def __init__(self, visitor: "MaskingCriticalDetectionVisitor") -> None:
        self.visitor: MaskingCriticalDetectionVisitor = visitor
        self.input_dependencies: dict[RelationalExpression, set[tuple[str, bool]]] = {}
        self.stack: list[set[tuple[str, bool]]] = []

    def reset(self) -> None:
        self.stack = []

    def visit_call_expression(self, expr: CallExpression) -> None:
        dependencies: set[tuple[str, bool]] = set()
        for input_expr in expr.inputs:
            input_expr.accept(self)
            dependencies.update(self.stack.pop())
        if isinstance(expr.op, pydop.MaskedExpressionFunctionOperator):
            dependencies.add(
                (
                    f"{expr.op.table_path}.{expr.op.masking_metadata.column_name}",
                    expr.op.is_unmask,
                )
            )
        self.stack.append(dependencies)

    def visit_window_expression(self, expr: WindowCallExpression) -> None:
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
        self.stack.append(self.input_dependencies.get(column_reference, set()))

    def visit_correlated_reference(
        self, correlated_reference: CorrelatedReference
    ) -> None:
        self.stack.append(set())

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        self.stack.append(set())


class MaskingCriticalDetectionVisitor(RelationalVisitor):
    """
    A visitor to XXX.
    """

    def __init__(self) -> None:
        self.critical_mask_columns: set[str] = set()
        self.critical_unmask_columns: set[str] = set()
        self.expression_visitor = MaskingCriticalDetectionExpressionVisitor(self)
        self.stack: list[dict[RelationalExpression, set[tuple[str, bool]]]] = []

    def reset(self) -> None:
        self.critical_mask_columns.clear()
        self.critical_unmask_columns.clear()
        self.stack.clear()
        self.expression_visitor.reset()

    def visit_inputs(self, node: RelationalNode) -> None:
        """
        TODO
        """
        input_dependencies: dict[RelationalExpression, set[tuple[str, bool]]] = {}
        for idx, input_node in reversed(list(enumerate(node.inputs))):
            input_node.accept(self)
            dependencies: dict[RelationalExpression, set[tuple[str, bool]]] = (
                self.stack.pop()
            )
            if len(node.inputs) == 1:
                input_dependencies = dependencies
            else:
                alias: str | None = node.default_input_aliases[idx]
                for expr, deps in dependencies.items():
                    input_dependencies[add_input_name(expr, alias)] = deps
        self.expression_visitor.input_dependencies = input_dependencies

    def find_critical_dependencies(self, expr: RelationalExpression) -> None:
        """
        TODO
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
        TODO
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
        TODO
        """
        logger = get_logger()
        for column in self.critical_mask_columns:
            print("*", column)
            logger.warning(
                f"Warning: query will not produce a valid output unless user has permission to mask column `{column}`"
            )
        for column in self.critical_unmask_columns:
            logger.warning(
                f"Warning: query will not produce a valid output unless user has permission to unmask column `{column}`"
            )

    def visit_project(self, project: Project) -> None:
        self.visit_inputs(project)
        self.add_output_dependencies(project)

    def visit_filter(self, filter: Filter) -> None:
        self.visit_inputs(filter)
        self.find_critical_dependencies(filter.condition)
        self.add_output_dependencies(filter)

    def visit_join(self, join: Join) -> None:
        self.visit_inputs(join)
        self.find_critical_dependencies(join.condition)
        self.add_output_dependencies(join)

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        self.visit_inputs(aggregate)
        for agg_key in aggregate.keys.values():
            self.find_critical_dependencies(agg_key)
        self.add_output_dependencies(aggregate)

    def visit_limit(self, limit: Limit) -> None:
        self.visit_inputs(limit)
        for order_expr in limit.orderings:
            self.find_critical_dependencies(order_expr.expr)
        self.add_output_dependencies(limit)

    def visit_root(self, root: RelationalRoot) -> None:
        self.visit_inputs(root)
        for order_expr in root.orderings:
            self.find_critical_dependencies(order_expr.expr)
        self.add_output_dependencies(root)

    def visit_scan(self, scan: Scan) -> None:
        self.expression_visitor.input_dependencies = {}
        self.add_output_dependencies(scan)

    def visit_empty_singleton(self, empty_singleton: EmptySingleton) -> None:
        self.expression_visitor.input_dependencies = {}
        self.add_output_dependencies(empty_singleton)
