"""
TODO: add file-level docstring
"""

__all__ = ["convert_ast_to_relational"]


from pydough.metadata import SimpleTableMetadata
from pydough.pydough_ast import (
    Calc,
    ChildOperator,
    ColumnProperty,
    CompoundSubCollection,
    GlobalContext,
    OrderBy,
    PartitionBy,
    PartitionChild,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    SubCollection,
    TableCollection,
    TopK,
    Where,
)
from pydough.relational.relational_expressions import (
    ColumnReference,
    ColumnSortInfo,
    RelationalExpression,
)
from pydough.relational.relational_nodes import Relational, RelationalRoot, Scan

translation_output = tuple[Relational, dict[PyDoughExpressionAST, ColumnReference]]


class RelTranslation:
    def translate_table_collection(
        self, node: TableCollection, desired_inputs: set[PyDoughExpressionAST]
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        output_mappings: dict[PyDoughExpressionAST, ColumnReference] = {}
        out_columns: dict[str, RelationalExpression] = {}
        assert isinstance(
            node.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {node.collection.__class__.__name__}"
        for expr in desired_inputs:
            assert isinstance(expr, ColumnProperty)
        answer = Scan(node.collection.table_path, out_columns)
        return answer, output_mappings

    def translate_sub_collection(
        self,
        node: SubCollection,
        desired_inputs: set[PyDoughExpressionAST],
        context: Relational,
        input_mapping: dict[PyDoughExpressionAST, ColumnReference],
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_compound_sub_collection(
        self,
        node: CompoundSubCollection,
        desired_inputs: set[PyDoughExpressionAST],
        context: Relational,
        input_mapping: dict[PyDoughExpressionAST, ColumnReference],
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_partition_child(
        self,
        node: PartitionChild,
        desired_inputs: set[PyDoughExpressionAST],
        context: Relational,
        input_mapping: dict[PyDoughExpressionAST, ColumnReference],
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_calc(
        self,
        node: Calc,
        desired_inputs: set[PyDoughExpressionAST],
        contexts: list[Relational],
        input_mapping: list[dict[PyDoughExpressionAST, ColumnReference]],
        child_offset: int,
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_where(
        self,
        node: Where,
        desired_inputs: set[PyDoughExpressionAST],
        contexts: list[Relational],
        input_mapping: list[dict[PyDoughExpressionAST, ColumnReference]],
        child_offset: int,
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_order_by(
        self,
        node: OrderBy,
        desired_inputs: set[PyDoughExpressionAST],
        contexts: list[Relational],
        input_mapping: list[dict[PyDoughExpressionAST, ColumnReference]],
        child_offset: int,
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_top_k(
        self,
        node: TopK,
        desired_inputs: set[PyDoughExpressionAST],
        contexts: list[Relational],
        input_mapping: list[dict[PyDoughExpressionAST, ColumnReference]],
        child_offset: int,
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_partition_by(
        self,
        node: PartitionBy,
        desired_inputs: set[PyDoughExpressionAST],
        contexts: list[Relational],
        input_mapping: list[dict[PyDoughExpressionAST, ColumnReference]],
        child_offset: int,
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def rel_translation(
        self, node: PyDoughCollectionAST, desired_inputs: set[PyDoughExpressionAST]
    ) -> translation_output:
        """
        TODO: add function docstring
        """
        contexts: list[Relational] = []
        input_mappings: list[dict[PyDoughExpressionAST, ColumnReference]] = []
        child_offset: int = 0
        if node.preceding_context is not None and not isinstance(
            node.preceding_context, GlobalContext
        ):
            desired_predecessor_terms: set[PyDoughExpressionAST] = {
                node.get_expr(name) for name in node.preceding_context.calc_terms
            }
            preceding_relational, preceding_mappings = self.rel_translation(
                node.preceding_context, desired_predecessor_terms
            )
            contexts.append(preceding_relational)
            input_mappings.append(preceding_mappings)
            child_offset = 1
        elif node.ancestor_context is not None and not isinstance(
            node.ancestor_context, GlobalContext
        ):
            desired_ancestor_terms: set[PyDoughExpressionAST] = {
                node.get_expr(name) for name in node.ancestor_context.calc_terms
            }
            ancestor_relational, ancestor_mappings = self.rel_translation(
                node.ancestor_context, desired_ancestor_terms
            )
            contexts.append(ancestor_relational)
            input_mappings.append(ancestor_mappings)
            child_offset = 1

        if isinstance(node, ChildOperator):
            if len(node.children) > 0:
                raise NotImplementedError(
                    "TODO: support child operators with 1+ children"
                )

        match node:
            case TableCollection():
                return self.translate_table_collection(node, desired_inputs)
            case CompoundSubCollection():
                assert len(contexts) == 1 and len(input_mappings) == 1
                return self.translate_compound_sub_collection(
                    node, desired_inputs, contexts[0], input_mappings[0]
                )
            case SubCollection():
                assert len(contexts) == 1 and len(input_mappings) == 1
                return self.translate_sub_collection(
                    node, desired_inputs, contexts[0], input_mappings[0]
                )
            case PartitionChild():
                assert len(contexts) == 1 and len(input_mappings) == 1
                return self.translate_partition_child(
                    node, desired_inputs, contexts[0], input_mappings[0]
                )
            case Calc():
                return self.translate_calc(
                    node, desired_inputs, contexts, input_mappings, child_offset
                )
            case Where():
                return self.translate_where(
                    node, desired_inputs, contexts, input_mappings, child_offset
                )
            case OrderBy():
                return self.translate_order_by(
                    node, desired_inputs, contexts, input_mappings, child_offset
                )
            case TopK():
                return self.translate_top_k(
                    node, desired_inputs, contexts, input_mappings, child_offset
                )
            case PartitionBy():
                return self.translate_partition_by(
                    node, desired_inputs, contexts, input_mappings, child_offset
                )
            case _:
                raise NotImplementedError(
                    f"TODO: support relational conversion on {node.__class__.__name__}"
                )


def convert_ast_to_relational(node: PyDoughCollectionAST) -> Relational:
    """
    TODO: add function docstring
    """
    desired_inputs: set[PyDoughExpressionAST] = set()
    for name in node.calc_terms:
        desired_inputs.add(node.get_expr(name))
    if node.ordering is not None:
        for expr in node.ordering:
            desired_inputs.add(expr.expr)
    ordered_columns: list[tuple[str, RelationalExpression]] = []
    orderings: list[ColumnSortInfo] | None = None
    rel_node, desired_input_mapping = RelTranslation().rel_translation(
        node, desired_inputs
    )
    for name in node.calc_terms:
        ast_expr: PyDoughExpressionAST = node.get_expr(name)
        rel_expr: RelationalExpression = desired_input_mapping[ast_expr]
        ordered_columns.append((name, rel_expr))
    ordered_columns.sort(key=lambda col: node.get_expression_position(col[0]))
    if node.ordering is not None:
        orderings = []
        for expr in node.ordering:
            relational_expr = desired_input_mapping[expr]
            if not isinstance(relational_expr, ColumnReference):
                raise NotImplementedError(
                    "TODO: support root ordering on expressions besides column references"
                )
            collation_expr: ColumnSortInfo = ColumnSortInfo(
                relational_expr, expr.asc, not expr.na_last
            )
            orderings.append(collation_expr)
    return RelationalRoot(rel_node, ordered_columns, orderings)
