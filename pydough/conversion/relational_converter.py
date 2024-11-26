"""
TODO: add file-level docstring
"""

__all__ = ["convert_ast_to_relational"]


from dataclasses import dataclass
from typing import Optional

import pydough.pydough_ast.pydough_operators as pydop
from pydough.metadata import (
    CartesianProductMetadata,
    SimpleJoinMetadata,
    SimpleTableMetadata,
)
from pydough.pydough_ast import (
    Calc,
    ChildOperator,
    ChildOperatorChildAccess,
    CollationExpression,
    ColumnProperty,
    CompoundSubCollection,
    GlobalContext,
    Literal,
    OrderBy,
    PartitionBy,
    PartitionChild,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Reference,
    SubCollection,
    TableCollection,
    TopK,
    Where,
)
from pydough.relational.relational_expressions import (
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    LiteralExpression,
    RelationalExpression,
)
from pydough.relational.relational_nodes import (
    Join,
    JoinType,
    Relational,
    RelationalRoot,
    Scan,
)
from pydough.types import BooleanType


@dataclass
class TranslationOutput:
    relation: Relational
    node: PyDoughCollectionAST
    expressions: dict[PyDoughExpressionAST, ColumnReference]


class HybridTree:
    def __init__(self, collection: PyDoughCollectionAST):
        self._collection: PyDoughCollectionAST = collection
        self._children: list[HybridTree] = []
        self._successor: HybridTree | None = None
        self._parent: HybridTree | None = None

    @property
    def collection(self) -> PyDoughCollectionAST:
        """
        TODO
        """
        return self._collection

    @property
    def children(self) -> list["HybridTree"]:
        """
        TODO
        """
        return self._children

    @property
    def successor(self) -> Optional["HybridTree"]:
        """
        TODO
        """
        return self._successor

    @property
    def parent(self) -> Optional["HybridTree"]:
        """
        TODO
        """
        return self._parent

    def __repr__(self):
        child_strings: list[str] = []
        for child in self.children + (
            [self.successor] if self.successor is not None else []
        ):
            for child_lines in repr(child).splitlines():
                child_strings.append(f" {child_lines}")
        return f"{self.collection.standalone_string}\n{'\n'.join(child_strings)}"

    def add_child(self, child: "HybridTree") -> None:
        """
        TODO
        """
        if child in self._children:
            raise Exception("Duplicate child")
        self._children.append(child)
        child._parent = self

    def add_successor(self, child: "HybridTree") -> None:
        """
        TODO
        """
        if self._successor is not None:
            raise Exception("Duplicate successor")
        self._successor = child
        child._parent = self

    @staticmethod
    def make_hybrid_tree(
        node: PyDoughCollectionAST,
        propagated_children: list["HybridTree"] | None = None,
        propagated_successor: Optional["HybridTree"] = None,
    ) -> "HybridTree":
        """
        TODO
        """
        hybrid: HybridTree = HybridTree(node)
        if propagated_successor is not None:
            hybrid.add_successor(propagated_successor)
        if propagated_children is not None:
            for propagated_child in propagated_children:
                hybrid.add_child(propagated_child)
        next_children: list[HybridTree] = []
        if isinstance(node, ChildOperatorChildAccess):
            return hybrid
        if isinstance(node, ChildOperator):
            for child in node.children:
                next_children.append(HybridTree.make_hybrid_tree(child))
        if node.preceding_context is not None:
            return HybridTree.make_hybrid_tree(
                node.preceding_context, next_children, hybrid
            )
        elif node.ancestor_context is not None:
            return HybridTree.make_hybrid_tree(
                node.ancestor_context, next_children, hybrid
            )
        else:
            return hybrid


class RelTranslation:
    def build_simple_table_scan(
        self, node: PyDoughCollectionAST, table_path: str
    ) -> TranslationOutput:
        """
        TODO: add function docstring
        """
        out_columns: dict[PyDoughExpressionAST, ColumnReference] = {}
        scan_columns: dict[str, RelationalExpression] = {}
        for expr_name in node.calc_terms:
            expr: PyDoughExpressionAST = node.get_expr(expr_name)
            assert isinstance(expr, ColumnProperty)
            scan_ref: ColumnReference = ColumnReference(
                expr.column_property.column_name, expr.column_property.data_type
            )
            out_ref: ColumnReference = ColumnReference(
                expr_name, expr.column_property.data_type
            )
            scan_columns[expr_name] = scan_ref
            out_columns[expr] = out_ref
        answer = Scan(table_path, scan_columns)
        return TranslationOutput(answer, node, out_columns)

    def translate_table_collection(
        self, node: TableCollection
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        assert isinstance(
            node.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {node.collection.__class__.__name__}"
        return [self.build_simple_table_scan(node, node.collection.table_path)]

    def translate_sub_collection(
        self,
        node: SubCollection,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        assert len(ancestry) > 0

        # First, build the table scan for the collection being stepped into.
        assert isinstance(
            node.collection, SimpleTableMetadata
        ), f"Expected table collection to correspond to an instance of simple table metadata, found: {node.collection.__class__.__name__}"
        rhs_output: TranslationOutput = self.build_simple_table_scan(
            node, node.collection.table_path
        )

        # Create the join node so we know what aliases it uses, but leave
        # the condition as always-True and the output columns empty for now.
        out_columns: dict[PyDoughExpressionAST, ColumnReference] = {}
        join_columns: dict[str, RelationalExpression] = {}
        out_rel: Join = Join(
            [ancestry[0].relation, rhs_output.relation],
            [LiteralExpression(True, BooleanType())],
            [JoinType.INNER],
            join_columns,
        )
        input_aliases: list[str | None] = out_rel.default_input_aliases

        if isinstance(node.subcollection_property, SimpleJoinMetadata):
            # If the subcollection is a simple join property, extract the keys
            # and build the corresponding (lhs_key == rhs_key) conditions
            cond_terms: list[RelationalExpression] = []
            for lhs_name in node.subcollection_property.keys:
                lhs_expr: PyDoughExpressionAST = ancestry[0].node.get_expr(lhs_name)
                lhs_key: ColumnReference = (
                    ancestry[0].expressions[lhs_expr].with_input(input_aliases[0])
                )
                for rhs_name in node.subcollection_property.keys[lhs_name]:
                    rhs_expr: PyDoughExpressionAST = node.get_expr(rhs_name)
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
        elif not isinstance(node.subcollection_property, CartesianProductMetadata):
            raise NotImplementedError()

        # Redecorate all of the predecessor terms with the LHS alias, and add
        # to the output columns of the JOIN node.
        for ancestor in ancestry:
            for expr in ancestor.expressions:
                new_ancestor_reference: ColumnReference = ancestor.expressions[
                    expr
                ].with_input(input_aliases[0])
                ancestor.expressions[expr] = new_ancestor_reference
                join_columns[new_ancestor_reference.name] = new_ancestor_reference
        for expr in rhs_output.expressions:
            old_reference = rhs_output.expressions[expr]
            old_name: str = old_reference.name
            new_name: str = old_name
            idx: int = 0
            while new_name in join_columns:
                new_name = f"{old_name}_{idx}"
                idx += 1
            new_reference: ColumnReference = ColumnReference(
                new_name, old_reference.data_type
            )
            join_columns[new_name] = old_reference.with_input(input_aliases[1])
            out_columns[expr] = new_reference
        ancestry.insert(0, TranslationOutput(out_rel, node, out_columns))
        return ancestry

    def translate_compound_sub_collection(
        self,
        node: CompoundSubCollection,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_partition_child(
        self,
        node: PartitionChild,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_calc(
        self,
        hybrid: HybridTree,
        node: Calc,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_where(
        self,
        hybrid: HybridTree,
        node: Where,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_order_by(
        self,
        hybrid: HybridTree,
        node: OrderBy,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_top_k(
        self,
        hybrid: HybridTree,
        node: TopK,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def translate_partition_by(
        self,
        hybrid: HybridTree,
        node: PartitionBy,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        raise NotImplementedError()

    def rel_translation(
        self,
        hybrid: HybridTree,
        ancestry: list[TranslationOutput],
    ) -> list[TranslationOutput]:
        """
        TODO: add function docstring
        """
        node: PyDoughCollectionAST = hybrid.collection
        output: list[TranslationOutput]
        match node:
            case TableCollection():
                output = self.translate_table_collection(node)
            case CompoundSubCollection():
                output = self.translate_compound_sub_collection(node, ancestry)
            case SubCollection():
                output = self.translate_sub_collection(node, ancestry)
            case PartitionChild():
                output = self.translate_partition_child(node, ancestry)
            case Calc():
                output = self.translate_calc(hybrid, node, ancestry)
            case Where():
                output = self.translate_where(hybrid, node, ancestry)
            case OrderBy():
                output = self.translate_order_by(hybrid, node, ancestry)
            case TopK():
                output = self.translate_top_k(hybrid, node, ancestry)
            case PartitionBy():
                output = self.translate_partition_by(hybrid, node, ancestry)
            case GlobalContext():
                output = []
            case _:
                raise NotImplementedError(
                    f"TODO: support relational conversion on {node.__class__.__name__}"
                )
        if hybrid.successor is None:
            return output
        return self.rel_translation(hybrid.successor, output)

    @staticmethod
    def preprocess_root(
        node: PyDoughCollectionAST,
    ) -> tuple[PyDoughCollectionAST, list[CollationExpression]]:
        """
        Transforms the final PyDough collection by appending it with an extra CALC
        """
        # Skip this step if every one of the original columns is a primitive
        # expression and there is either no ordering, or all of the ordering
        # keys is also one of the final columns.
        original_calc_terms: set[str] = node.calc_terms
        original_expressions: set[PyDoughExpressionAST] = {
            node.get_expr(name) for name in original_calc_terms
        }
        if all(
            isinstance(expr, (Reference, Literal, ColumnProperty))
            for expr in original_expressions
        ) and (
            node.ordering is None
            or all(
                collation.expr in original_expressions for collation in node.ordering
            )
        ):
            return node, [] if node.ordering is None else node.ordering

        # Fetch all of the expressions that should be kept in the final output
        children: list[PyDoughCollectionAST] = []
        final_terms: list[tuple[str, PyDoughExpressionAST]] = []
        all_names: set[str] = set()
        for name in original_calc_terms:
            final_terms.append((name, Reference(node, name)))
            all_names.add(name)
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


def convert_ast_to_relational(node: PyDoughCollectionAST) -> Relational:
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
    hybrid: HybridTree = HybridTree.make_hybrid_tree(node)
    output: list[TranslationOutput] = translator.rel_translation(hybrid, [])
    assert len(output) > 0
    ordered_columns: list[tuple[str, RelationalExpression]] = []
    orderings: list[ExpressionSortInfo] | None = None

    # Extract the relevant expressions for the final columns and ordering keys
    # so that the root node can be built from them.
    for name in final_terms:
        expr: PyDoughExpressionAST = node.get_expr(name)
        rel_expr: RelationalExpression = output[0].expressions[expr]
        ordered_columns.append((name, rel_expr))
    ordered_columns.sort(key=lambda col: node.get_expression_position(col[0]))
    if collation is not None:
        orderings = []
        for col_expr in collation:
            relational_expr = output[0].expressions[col_expr.expr]
            if not isinstance(relational_expr, ColumnReference):
                raise NotImplementedError(
                    "TODO: support root ordering on expressions besides column references"
                )
            collation_expr: ExpressionSortInfo = ExpressionSortInfo(
                relational_expr, col_expr.asc, not col_expr.na_last
            )
            orderings.append(collation_expr)
    return RelationalRoot(output[0].relation, ordered_columns, orderings)
