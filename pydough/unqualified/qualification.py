"""
TODO: add file-level docstring
"""

__all__ = ["qualify_node"]

import re
from collections.abc import MutableSequence
from functools import cache

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import (
    AstNodeBuilder,
    Calc,
    ChildOperatorChildAccess,
    ChildReference,
    CollationExpression,
    OrderBy,
    PartitionBy,
    PyDoughAST,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Reference,
    TopK,
    Where,
)
from pydough.pydough_ast.pydough_operators.expression_operators.binary_operators import (
    BinOp,
)
from pydough.types import PyDoughType

from .errors import PyDoughUnqualifiedException
from .unqualified_node import (
    UnqualifiedAccess,
    UnqualifiedBack,
    UnqualifiedBinaryOperation,
    UnqualifiedCalc,
    UnqualifiedCollation,
    UnqualifiedLiteral,
    UnqualifiedNode,
    UnqualifiedOperation,
    UnqualifiedOrderBy,
    UnqualifiedPartition,
    UnqualifiedRoot,
    UnqualifiedTopK,
    UnqualifiedWhere,
)


class Qualifier:
    not_expression_pattern: re.Pattern = re.compile(
        "Cannot qualify (\S*) as an expression"
    )

    def __init__(self, graph: GraphMetadata):
        self._graph: GraphMetadata = graph
        self._builder: AstNodeBuilder = AstNodeBuilder(graph)
        self._memo: dict[tuple[str, PyDoughCollectionAST | None], PyDoughAST] = {}

    @property
    def graph(self) -> GraphMetadata:
        """
        TODO: add function docstring
        """
        return self._graph

    @property
    def builder(self) -> AstNodeBuilder:
        """
        TODO: add function docstring
        """
        return self._builder

    def lookup_if_already_qualified(
        self,
        unqualified_str: str,
        context: PyDoughCollectionAST | None,
    ) -> PyDoughAST | None:
        """
        TODO: add function docstring
        """
        return self._memo.get((unqualified_str, context), None)

    def add_definition(
        self,
        unqualified_str: str,
        context: PyDoughCollectionAST | None,
        qualified_node: PyDoughAST,
    ):
        """
        TODO: add function docstring
        """
        self._memo[unqualified_str, context] = qualified_node

    def qualify_expression(
        self,
        unqualified: UnqualifiedNode,
        context: PyDoughCollectionAST,
        children: list[PyDoughCollectionAST],
    ) -> PyDoughExpressionAST:
        """
        Transforms an `UnqualifiedNode` into a PyDoughExpressionAST node.

        Args:
            `unqualified`: the UnqualifiedNode instance to be transformed.
            `context`: the collection AST whose context the expression is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.

        Returns:
            The PyDough AST object for the qualified expression node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughASTException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_str: str = str(unqualified)
        lookup: PyDoughAST | None = self.lookup_if_already_qualified(
            unqualified_str, context
        )
        bad_expression: Exception = PyDoughUnqualifiedException(
            f"Cannot qualify {unqualified.__class__.__name__} as an expression: {unqualified!r}"
        )
        if lookup is not None:
            if not isinstance(lookup, PyDoughExpressionAST):
                raise bad_expression
            return lookup
        unqualified_expr: UnqualifiedNode
        qualified_expr: PyDoughExpressionAST
        answer: PyDoughExpressionAST
        operation: str | None
        name: str
        match unqualified:
            case UnqualifiedLiteral():
                value: object = unqualified._parcel[0]
                data_type: PyDoughType = unqualified._parcel[1]
                answer = self.builder.build_literal(value, data_type)
            case UnqualifiedOperation():
                operation = unqualified._parcel[0]
                unqualified_operands: MutableSequence[UnqualifiedNode] = (
                    unqualified._parcel[1]
                )
                qualified_operands: MutableSequence[PyDoughAST] = []
                for node in unqualified_operands:
                    try:
                        qualified_operands.append(
                            self.qualify_expression(node, context, children)
                        )
                    except PyDoughUnqualifiedException as e:
                        if self.not_expression_pattern.match(str(e)):
                            qualified_operands.append(
                                self.qualify_collection(node, context, True)
                            )
                        else:
                            raise e
                answer = self.builder.build_expression_function_call(
                    operation, qualified_operands
                )
            case UnqualifiedBinaryOperation():
                operator: str = unqualified._parcel[0]
                unqualified_lhs: UnqualifiedNode = unqualified._parcel[1]
                unqualified_rhs: UnqualifiedNode = unqualified._parcel[2]
                qualified_lhs: PyDoughExpressionAST = self.qualify_expression(
                    unqualified_lhs, context, children
                )
                qualified_rhs: PyDoughExpressionAST = self.qualify_expression(
                    unqualified_rhs, context, children
                )
                operation = None
                for _, op in BinOp.__members__.items():
                    if operator == op.value:
                        operation = op.name
                assert operation is not None, f"Unknown binary operation {operator!r}"
                answer = self.builder.build_expression_function_call(
                    operation, [qualified_lhs, qualified_rhs]
                )
            case UnqualifiedCollation():
                unqualified_expr = unqualified._parcel[0]
                asc = unqualified._parcel[1]
                na_last = unqualified._parcel[2]
                qualified_expr = self.qualify_expression(
                    unqualified_expr, context, children
                )
                answer = CollationExpression(qualified_expr, asc, na_last)
            case UnqualifiedAccess():
                unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
                name = unqualified._parcel[1]
                if isinstance(unqualified_parent, UnqualifiedBack):
                    levels: int = unqualified_parent._parcel[0]
                    answer = self.builder.build_back_reference_expression(
                        context, name, levels
                    )
                else:
                    qualified_parent: PyDoughCollectionAST = self.qualify_collection(
                        unqualified_parent, context, True
                    )
                    term: PyDoughAST = qualified_parent.get_term(name)
                    if not isinstance(term, PyDoughExpressionAST):
                        raise bad_expression
                    if isinstance(unqualified_parent, UnqualifiedRoot):
                        answer = self.builder.build_reference(context, name)
                    else:
                        ref_num: int
                        if qualified_parent in children:
                            ref_num = children.index(qualified_parent)
                        else:
                            ref_num = len(children)
                            children.append(qualified_parent)
                        answer = self.builder.build_child_reference(
                            children, ref_num, name
                        )
            case _:
                raise bad_expression
        self.add_definition(unqualified_str, context, answer)
        return answer

    @cache
    def qualify_collection(
        self,
        unqualified: UnqualifiedNode,
        context: PyDoughCollectionAST,
        is_child: bool,
    ) -> PyDoughCollectionAST:
        """
        Transforms an `UnqualifiedNode` into a PyDoughCollectionAST node.

        Args:
            `unqualified`: the UnqualifiedNode instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection AST whose context the collection is being
            evaluated within.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALC or PARTITION.

        Returns:
            The PyDough AST object for the qualified collection node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughASTException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_str: str = str(unqualified)
        lookup: PyDoughAST | None = self.lookup_if_already_qualified(
            unqualified_str, context
        )
        bad_collection: Exception = PyDoughUnqualifiedException(
            f"Cannot qualify {unqualified.__class__.__name__} as a collection: {unqualified!r}"
        )
        if lookup is not None:
            if not isinstance(lookup, PyDoughCollectionAST):
                raise bad_collection
            return lookup
        answer: PyDoughCollectionAST
        name: str
        unqualified_parent: UnqualifiedNode
        qualified_parent: PyDoughCollectionAST
        children: MutableSequence[PyDoughCollectionAST]
        unqualified_terms: MutableSequence[tuple[str, UnqualifiedNode]]
        unqualified_nameless_terms: MutableSequence[UnqualifiedNode]
        qualified_terms: MutableSequence[tuple[str, PyDoughExpressionAST]]
        qualified_collations: MutableSequence[CollationExpression]
        qualified_term: PyDoughExpressionAST
        match unqualified:
            case UnqualifiedRoot():
                answer = context
            case UnqualifiedAccess():
                unqualified_parent = unqualified._parcel[0]
                name = unqualified._parcel[1]
                if isinstance(unqualified_parent, UnqualifiedBack):
                    levels: int = unqualified_parent._parcel[0]
                    answer = self.builder.build_back_reference_collection(
                        context, name, levels
                    )
                else:
                    qualified_parent = self.qualify_collection(
                        unqualified_parent, context, is_child
                    )
                    answer = self.builder.build_child_access(name, qualified_parent)
                    if isinstance(unqualified_parent, UnqualifiedRoot) and is_child:
                        answer = ChildOperatorChildAccess(answer)
            case UnqualifiedCalc():
                unqualified_parent = unqualified._parcel[0]
                unqualified_terms = unqualified._parcel[1]
                qualified_parent = self.qualify_collection(
                    unqualified_parent, context, is_child
                )
                children = []
                qualified_terms = []
                for name, term in unqualified_terms:
                    qualified_term = self.qualify_expression(
                        term, qualified_parent, children
                    )
                    qualified_terms.append((name, qualified_term))
                calc: Calc = self.builder.build_calc(qualified_parent, children)
                answer = calc.with_terms(qualified_terms)
            case UnqualifiedWhere():
                unqualified_parent = unqualified._parcel[0]
                unqualified_cond: UnqualifiedNode = unqualified._parcel[1]
                qualified_parent = self.qualify_collection(
                    unqualified_parent, context, is_child
                )
                children = []
                qualified_term = self.qualify_expression(
                    unqualified_cond, qualified_parent, children
                )
                where: Where = self.builder.build_where(qualified_parent, children)
                answer = where.with_condition(qualified_term)
            case UnqualifiedOrderBy():
                unqualified_parent = unqualified._parcel[0]
                unqualified_nameless_terms = unqualified._parcel[1]
                qualified_parent = self.qualify_collection(
                    unqualified_parent, context, is_child
                )
                children = []
                qualified_collations = []
                for term in unqualified_nameless_terms:
                    qualified_term = self.qualify_expression(
                        term, qualified_parent, children
                    )
                    assert isinstance(qualified_term, CollationExpression)
                    qualified_collations.append(qualified_term)
                orderby: OrderBy = self.builder.build_order(qualified_parent, children)
                answer = orderby.with_collation(qualified_collations)
            case UnqualifiedTopK():
                unqualified_parent = unqualified._parcel[0]
                records_to_keep: int = unqualified._parcel[1]
                # TODO: add ability to infer the "by" clause from a predecessor
                assert unqualified._parcel[2] is not None
                unqualified_nameless_terms = unqualified._parcel[2]
                qualified_parent = self.qualify_collection(
                    unqualified_parent, context, is_child
                )
                children = []
                qualified_collations = []
                for term in unqualified_nameless_terms:
                    qualified_term = self.qualify_expression(
                        term, qualified_parent, children
                    )
                    assert isinstance(qualified_term, CollationExpression)
                    qualified_collations.append(qualified_term)
                topk: TopK = self.builder.build_top_k(
                    qualified_parent, children, records_to_keep
                )
                answer = topk.with_collation(qualified_collations)
            case UnqualifiedPartition():
                unqualified_parent = unqualified._parcel[0]
                unqualified_child: UnqualifiedNode = unqualified._parcel[1]
                child_name: str = unqualified._parcel[2]
                unqualified_nameless_terms = unqualified._parcel[3]
                qualified_parent = self.qualify_collection(
                    unqualified_parent, context, is_child
                )
                qualified_child: PyDoughCollectionAST = self.qualify_collection(
                    unqualified_child, qualified_parent, True
                )
                child_references: list[ChildReference] = []
                children = []
                for term in unqualified_nameless_terms:
                    qualified_term = self.qualify_expression(
                        term, qualified_child, children
                    )
                    assert isinstance(
                        qualified_term, Reference
                    ), "PARTITION currently only supports partition keys that are references to a scalar property of the collection being partitioned"
                    child_ref: ChildReference = ChildReference(
                        qualified_child, 0, qualified_term.term_name
                    )
                    child_references.append(child_ref)
                partition: PartitionBy = self.builder.build_partition(
                    qualified_parent, qualified_child, child_name
                )
                answer = partition.with_keys(child_references)
            case _:
                raise bad_collection
        self.add_definition(unqualified_str, context, answer)
        return answer


def qualify_node(
    unqualified: UnqualifiedNode, graph: GraphMetadata
) -> PyDoughCollectionAST:
    """
    Transforms an `UnqualifiedNode` into a qualified node.

    Args:
        `unqualified`: the UnqualifiedNode instance to be transformed.
        `graph`: the metadata for the graph that the PyDough computations
        are occurring within.

    Returns:
        The PyDough AST object for the qualified node.

    Raises:
        `PyDoughUnqualifiedException` or `PyDoughASTException` if something
        goes wrong during the qualification process, e.g. a term cannot be
        qualified or is not recognized.
    """
    qual: Qualifier = Qualifier(graph)
    return qual.qualify_collection(
        unqualified, qual.builder.build_global_context(), False
    )
