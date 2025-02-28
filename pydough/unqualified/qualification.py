"""
Implementations of the process for transforming unqualified nodes to PyDough
QDAG nodes.
"""

__all__ = ["qualify_node"]

from collections.abc import Iterable, MutableSequence

from pydough.metadata import GraphMetadata
from pydough.pydough_operators.expression_operators import (
    BinOp,
    ExpressionWindowOperator,
)
from pydough.qdag import (
    AstNodeBuilder,
    Calculate,
    ChildOperatorChildAccess,
    ChildReferenceExpression,
    CollationExpression,
    GlobalContext,
    Literal,
    OrderBy,
    PartitionBy,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    PyDoughQDAG,
    Reference,
    TopK,
    Where,
)
from pydough.types import PyDoughType

from .errors import PyDoughUnqualifiedException
from .unqualified_node import (
    UnqualifiedAccess,
    UnqualifiedBinaryOperation,
    UnqualifiedCalculate,
    UnqualifiedCollation,
    UnqualifiedLiteral,
    UnqualifiedNode,
    UnqualifiedOperation,
    UnqualifiedOrderBy,
    UnqualifiedPartition,
    UnqualifiedRoot,
    UnqualifiedTopK,
    UnqualifiedWhere,
    UnqualifiedWindow,
    display_raw,
)


class Qualifier:
    def __init__(self, graph: GraphMetadata):
        self._graph: GraphMetadata = graph
        self._builder: AstNodeBuilder = AstNodeBuilder(graph)
        self._memo: dict[tuple[str, PyDoughCollectionQDAG], PyDoughQDAG] = {}

    @property
    def graph(self) -> GraphMetadata:
        """
        The metadata for the PyDough graph in which is used to identify
        collections and properties.
        """
        return self._graph

    @property
    def builder(self) -> AstNodeBuilder:
        """
        The builder used by the qualifier to create QDAG nodes.
        """
        return self._builder

    def lookup_if_already_qualified(
        self,
        unqualified_str: str,
        context: PyDoughCollectionQDAG,
    ) -> PyDoughQDAG | None:
        """
        Fetches the qualified definition of an unqualified node (by string) if
        it has already been defined within a certain context. Returns None
        if it has not.

        Args:
            `unqualified_str`: the string representation of the unqualified
            node (used for lookups since true equality is unsupported on
            unqualified nodes).
            `context`: the collection context in which the qualification is
            being done.

        Returns:
            The stored answer if one has already been computed, otherwise None.
        """
        return self._memo.get((unqualified_str, context), None)

    def add_definition(
        self,
        unqualified_str: str,
        context: PyDoughCollectionQDAG,
        qualified_node: PyDoughQDAG,
    ):
        """
        Persists the qualified definition of an unqualified node (by string)
        once it has been defined within a certain context so that the answer
        can be instantly fetched if required again..

        Args:
            `unqualified_str`: the string representation of the unqualified
            node (used for lookups since true equality is unsupported on
            unqualified nodes).
            `context`: the collection context in which the qualification is
            being done.
            `qualifeid_node`: the qualified definition of the unqualified node
            when placed within the context.
        """
        self._memo[unqualified_str, context] = qualified_node

    def qualify_literal(self, unqualified: UnqualifiedLiteral) -> PyDoughExpressionQDAG:
        """
        Transforms an `UnqualifiedLiteral` into a PyDoughExpressionQDAG node.

        Args:
            `unqualified`: the UnqualifiedLiteral instance to be transformed.
            `context`: the collection QDAG whose context the expression is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.

        Returns:
            The PyDough QDAG object for the qualified expression node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        value: object = unqualified._parcel[0]
        data_type: PyDoughType = unqualified._parcel[1]
        if isinstance(value, (list, tuple)):
            literal_elems: list[object] = []
            for elem in value:
                assert isinstance(elem, UnqualifiedLiteral)
                expr: PyDoughExpressionQDAG = self.qualify_literal(elem)
                assert isinstance(expr, Literal)
                literal_elems.append(expr.value)
            return self.builder.build_literal(literal_elems, data_type)
        return self.builder.build_literal(value, data_type)

    def qualify_operation(
        self,
        unqualified: UnqualifiedOperation,
        context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ) -> PyDoughExpressionQDAG:
        """
        Transforms an `UnqualifiedOperation` into a PyDoughExpressionQDAG node.

        Args:
            `unqualified`: the UnqualifiedOperation instance to be transformed.
            `context`: the collection QDAG whose context the expression is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.

        Returns:
            The PyDough QDAG object for the qualified expression node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        operation: str = unqualified._parcel[0]
        unqualified_operands: MutableSequence[UnqualifiedNode] = unqualified._parcel[1]
        qualified_operands: list[PyDoughQDAG] = []
        # Iterate across every operand to generate its qualified variant.
        # First, attempt to qualify it as an expression (the common case), but
        # if that fails specifically because the result would be a collection,
        # then attempt to qualify it as a collection.
        for node in unqualified_operands:
            operand: PyDoughQDAG = self.qualify_node(node, context, children, True)
            if isinstance(operand, PyDoughExpressionQDAG):
                qualified_operands.append(
                    self.qualify_expression(node, context, children)
                )
            else:
                assert isinstance(operand, PyDoughCollectionQDAG)
                # If the operand could be qualified as a collection, then
                # add it to the children list (if not already present) and
                # use a child reference collection as the argument.
                ref_num: int
                if operand in children:
                    ref_num = children.index(operand)
                else:
                    ref_num = len(children)
                    children.append(operand)
                child_collection_ref: PyDoughCollectionQDAG = (
                    self.builder.build_child_reference_collection(
                        context, children, ref_num
                    )
                )
                qualified_operands.append(child_collection_ref)
        return self.builder.build_expression_function_call(
            operation, qualified_operands
        )

    def qualify_binary_operation(
        self,
        unqualified: UnqualifiedBinaryOperation,
        context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ) -> PyDoughExpressionQDAG:
        """
        Transforms an `UnqualifiedBinaryOperation` into a PyDoughExpressionQDAG node.

        Args:
            `unqualified`: the UnqualifiedBinaryOperation instance to be transformed.
            `context`: the collection QDAG whose context the expression is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.

        Returns:
            The PyDough QDAG object for the qualified expression node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        operator: str = unqualified._parcel[0]
        # Iterate across all the values of the BinOp enum to figure out which
        # one correctly matches the BinOp specified by the operator.
        operation: str | None = None
        for _, op in BinOp.__members__.items():
            if operator == op.value:
                operation = op.name
        assert operation is not None, f"Unknown binary operation {operator!r}"
        # Independently qualify the LHS and RHS arguments
        unqualified_lhs: UnqualifiedNode = unqualified._parcel[1]
        unqualified_rhs: UnqualifiedNode = unqualified._parcel[2]
        qualified_lhs: PyDoughExpressionQDAG = self.qualify_expression(
            unqualified_lhs, context, children
        )
        qualified_rhs: PyDoughExpressionQDAG = self.qualify_expression(
            unqualified_rhs, context, children
        )
        return self.builder.build_expression_function_call(
            operation, [qualified_lhs, qualified_rhs]
        )

    def qualify_window(
        self,
        unqualified: UnqualifiedWindow,
        context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ) -> PyDoughExpressionQDAG:
        """
        Transforms an `UnqualifiedWindow` into a PyDoughExpressionQDAG node.

        Args:
            `unqualified`: the UnqualifiedWindow instance to be transformed.
            `context`: the collection QDAG whose context the expression is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.

        Returns:
            The PyDough QDAG object for the qualified window node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        window_operator: ExpressionWindowOperator = unqualified._parcel[0]
        unqualified_by: Iterable[UnqualifiedNode] = unqualified._parcel[1]
        levels: int | None = unqualified._parcel[2]
        kwargs: dict[str, object] = unqualified._parcel[3]
        # Qualify all of the collation terms, storing the children built along
        # the way.
        qualified_collations: list[CollationExpression] = []
        for term in unqualified_by:
            qualified_term: PyDoughExpressionQDAG = self.qualify_expression(
                term, context, children
            )
            assert isinstance(qualified_term, CollationExpression)
            qualified_collations.append(qualified_term)
        # Use the qualified children & collation to create a new ORDER BY node.
        if not qualified_collations:
            raise PyDoughUnqualifiedException(
                "Window calls require a non-empty 'by' clause to be specified."
            )
        return self.builder.build_window_call(
            window_operator, qualified_collations, levels, kwargs
        )

    def qualify_collation(
        self,
        unqualified: UnqualifiedCollation,
        context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ) -> PyDoughExpressionQDAG:
        """
        Transforms an `UnqualifiedCollation` into a PyDoughExpressionQDAG node.

        Args:
            `unqualified`: the UnqualifiedCollation instance to be transformed.
            `context`: the collection QDAG whose context the expression is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.

        Returns:
            The PyDough QDAG object for the qualified expression node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_expr: UnqualifiedNode = unqualified._parcel[0]
        asc: bool = unqualified._parcel[1]
        na_last: bool = unqualified._parcel[2]
        # Qualify the underlying expression, then wrap it in a collation.
        qualified_expr: PyDoughExpressionQDAG = self.qualify_expression(
            unqualified_expr, context, children
        )
        return CollationExpression(qualified_expr, asc, na_last)

    def qualify_access(
        self,
        unqualified: UnqualifiedAccess,
        context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
        is_child: bool,
    ) -> PyDoughQDAG:
        """
        Transforms an `UnqualifiedAccess` into a PyDough QDAG node, either as
        accessing a subcollection or an expression from the current context.

        Args:
            `unqualified`: the UnqualifiedAccess instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection QDAG whose context the collection is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALCULATE or PARTITION.

        Returns:
            The PyDough QDAG object for the qualified collection or expression
            node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        name: str = unqualified._parcel[1]
        term: PyDoughQDAG
        # First, qualify the parent collection.
        qualified_parent: PyDoughCollectionQDAG = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        if (
            isinstance(qualified_parent, GlobalContext)
            and name == qualified_parent.graph.name
        ):
            # Special case: if the parent is the root context and the child
            # is named after the graph name, return the parent since the
            # child is just a de-sugared invocation of the global context.
            return qualified_parent
        else:
            # Identify whether the access is an expression or a collection
            term = qualified_parent.get_term(name)
            if isinstance(term, PyDoughCollectionQDAG):
                # If it is a collection that is not the special case,
                # access the child collection from the qualified parent
                # collection.
                answer: PyDoughCollectionQDAG = self.builder.build_child_access(
                    name, qualified_parent
                )
                if isinstance(unqualified_parent, UnqualifiedRoot) and is_child:
                    answer = ChildOperatorChildAccess(answer)
                return answer
            else:
                assert isinstance(term, PyDoughExpressionQDAG)
                if isinstance(unqualified_parent, UnqualifiedRoot):
                    # If at the root, the access must be a reference to a scalar
                    # attribute accessible in the current context.
                    return self.builder.build_reference(context, name)
                else:
                    # Otherwise, the access is a reference to a scalar attribute of
                    # a child collection node of the current context. Add this new
                    # child to the list of children, unless already present, then
                    # return the answer as a reference to a field of the child.
                    ref_num: int
                    if qualified_parent in children:
                        ref_num = children.index(qualified_parent)
                    else:
                        ref_num = len(children)
                        children.append(qualified_parent)
                    return self.builder.build_child_reference_expression(
                        children, ref_num, name
                    )

    def qualify_calculate(
        self,
        unqualified: UnqualifiedCalculate,
        context: PyDoughCollectionQDAG,
        is_child: bool,
    ) -> PyDoughCollectionQDAG:
        """
        Transforms an `UnqualifiedCalculate` into a PyDoughCollectionQDAG node.

        Args:
            `unqualified`: the UnqualifiedCalculate instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection QDAG whose context the collection is being
            evaluated within.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALCULATE or PARTITION.

        Returns:
            The PyDough QDAG object for the qualified collection node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        unqualified_terms: MutableSequence[tuple[str, UnqualifiedNode]] = (
            unqualified._parcel[1]
        )
        qualified_parent: PyDoughCollectionQDAG = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        # Qualify all of the CALCULATE terms, storing the children built along
        # the way.
        children: MutableSequence[PyDoughCollectionQDAG] = []
        qualified_terms: MutableSequence[tuple[str, PyDoughExpressionQDAG]] = []
        for name, term in unqualified_terms:
            qualified_term = self.qualify_expression(term, qualified_parent, children)
            qualified_terms.append((name, qualified_term))
        # Use the qualified children & terms to create a new CALCULATE node.
        calculate: Calculate = self.builder.build_calculate(qualified_parent, children)
        return calculate.with_terms(qualified_terms)

    def qualify_where(
        self,
        unqualified: UnqualifiedWhere,
        context: PyDoughCollectionQDAG,
        is_child: bool,
    ) -> PyDoughCollectionQDAG:
        """
        Transforms an `UnqualifiedWhere` into a PyDoughCollectionQDAG node.

        Args:
            `unqualified`: the UnqualifiedWhere instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection QDAG whose context the collection is being
            evaluated within.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALCULATE or PARTITION.

        Returns:
            The PyDough QDAG object for the qualified collection node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        unqualified_cond: UnqualifiedNode = unqualified._parcel[1]
        qualified_parent: PyDoughCollectionQDAG = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        # Qualify the condition of the WHERE clause, storing the children
        # built along the way.
        children: MutableSequence[PyDoughCollectionQDAG] = []
        qualified_cond = self.qualify_expression(
            unqualified_cond, qualified_parent, children
        )
        # Use the qualified children & condition to create a new WHERE node.
        where: Where = self.builder.build_where(qualified_parent, children)
        return where.with_condition(qualified_cond)

    def qualify_order_by(
        self,
        unqualified: UnqualifiedOrderBy,
        context: PyDoughCollectionQDAG,
        is_child: bool,
    ) -> PyDoughCollectionQDAG:
        """
        Transforms an `UnqualifiedOrderBy` into a PyDoughCollectionQDAG node.

        Args:
            `unqualified`: the UnqualifiedOrderBy instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection QDAG whose context the collection is being
            evaluated within.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALCULATE or PARTITION.

        Returns:
            The PyDough QDAG object for the qualified collection node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        unqualified_terms: MutableSequence[UnqualifiedNode] = unqualified._parcel[1]
        qualified_parent: PyDoughCollectionQDAG = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        # Qualify all of the collation terms, storing the children built along
        # the way.
        children: MutableSequence[PyDoughCollectionQDAG] = []
        qualified_collations: list[CollationExpression] = []
        for term in unqualified_terms:
            qualified_term: PyDoughExpressionQDAG = self.qualify_expression(
                term, qualified_parent, children
            )
            assert isinstance(qualified_term, CollationExpression)
            qualified_collations.append(qualified_term)
        # Use the qualified children & collation to create a new ORDER BY node.
        if not qualified_collations:
            raise PyDoughUnqualifiedException(
                "ORDER BY requires a 'by' clause to be specified."
            )
        orderby: OrderBy = self.builder.build_order(qualified_parent, children)
        return orderby.with_collation(qualified_collations)

    def qualify_top_k(
        self,
        unqualified: UnqualifiedTopK,
        context: PyDoughCollectionQDAG,
        is_child: bool,
    ) -> PyDoughCollectionQDAG:
        """
        Transforms an `UnqualifiedTopK` into a PyDoughCollectionQDAG node.

        Args:
            `unqualified`: the UnqualifiedTopK instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection QDAG whose context the collection is being
            evaluated within.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALCULATE or PARTITION.

        Returns:
            The PyDough QDAG object for the qualified collection node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        records_to_keep: int = unqualified._parcel[1]
        # TODO: (gh #164) add ability to infer the "by" clause from a
        # predecessor
        assert (
            unqualified._parcel[2] is not None
        ), "TopK does not currently support an implied 'by' clause."
        unqualified_terms: MutableSequence[UnqualifiedNode] = unqualified._parcel[2]
        qualified_parent: PyDoughCollectionQDAG = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        # Qualify all of the collation terms, storing the children built along
        # the way.
        children: MutableSequence[PyDoughCollectionQDAG] = []
        qualified_collations: list[CollationExpression] = []
        for term in unqualified_terms:
            qualified_term: PyDoughExpressionQDAG = self.qualify_expression(
                term, qualified_parent, children
            )
            assert isinstance(qualified_term, CollationExpression)
            qualified_collations.append(qualified_term)
        if not qualified_collations:
            raise PyDoughUnqualifiedException(
                "TopK requires a 'by' clause to be specified."
            )
        # Use the qualified children & collation to create a new TOP K node.
        topk: TopK = self.builder.build_top_k(
            qualified_parent, children, records_to_keep
        )
        return topk.with_collation(qualified_collations)

    def qualify_partition(
        self,
        unqualified: UnqualifiedPartition,
        context: PyDoughCollectionQDAG,
        is_child: bool,
    ) -> PyDoughCollectionQDAG:
        """
        Transforms an `UnqualifiedPartition` into a PyDoughCollectionQDAG node.

        Args:
            `unqualified`: the UnqualifiedPartition instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection QDAG whose context the collection is being
            evaluated within.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALCULATE or PARTITION.

        Returns:
            The PyDough QDAG object for the qualified collection node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        unqualified_child: UnqualifiedNode = unqualified._parcel[1]
        child_name: str = unqualified._parcel[2]
        unqualified_terms: MutableSequence[UnqualifiedNode] = unqualified._parcel[3]
        # Qualify all both the parent collection and the child that is being
        # partitioned, using the qualified parent as the context for the
        # child.
        qualified_parent: PyDoughCollectionQDAG = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        qualified_child: PyDoughCollectionQDAG = self.qualify_collection(
            unqualified_child, qualified_parent, True
        )
        # Qualify all of the partitioning keys (which, for now, can only be
        # references to expressions in the child), storing the children built
        # along the way (which should just be the child input).
        child_references: list[ChildReferenceExpression] = []
        children: MutableSequence[PyDoughCollectionQDAG] = []
        for term in unqualified_terms:
            qualified_term: PyDoughExpressionQDAG = self.qualify_expression(
                term, qualified_child, children
            )
            assert isinstance(
                qualified_term, Reference
            ), "PARTITION currently only supports partition keys that are references to a scalar property of the collection being partitioned"
            child_ref: ChildReferenceExpression = ChildReferenceExpression(
                qualified_child, 0, qualified_term.term_name
            )
            child_references.append(child_ref)
        # Use the qualified child & keys to create a new PARTITION node.
        partition: PartitionBy = self.builder.build_partition(
            qualified_parent, qualified_child, child_name
        )
        partition = partition.with_keys(child_references)
        # Special case: if accessing as a child, wrap in a
        # ChildOperatorChildAccess term.
        if isinstance(unqualified_parent, UnqualifiedRoot) and is_child:
            return ChildOperatorChildAccess(partition)
        return partition

    def qualify_collection(
        self,
        unqualified: UnqualifiedNode,
        context: PyDoughCollectionQDAG,
        is_child: bool,
    ) -> PyDoughCollectionQDAG:
        """
        Transforms an `UnqualifiedNode` into a PyDoughCollectionQDAG node.

        Args:
            `unqualified`: the UnqualifiedNode instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection QDAG whose context the collection is being
            evaluated within.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALCULATE or PARTITION.

        Returns:
            The PyDough QDAG object for the qualified collection node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        answer: PyDoughQDAG = self.qualify_node(unqualified, context, [], is_child)
        if not isinstance(answer, PyDoughCollectionQDAG):
            raise PyDoughUnqualifiedException(
                f"Expected a collection, but received an expression: {answer}"
            )
        return answer

    def qualify_expression(
        self,
        unqualified: UnqualifiedNode,
        context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ) -> PyDoughExpressionQDAG:
        """
        Transforms an `UnqualifiedNode` into a PyDoughExpressionQDAG node.

        Args:
            `unqualified`: the UnqualifiedNode instance to be transformed.
            `context`: the collection QDAG whose context the expression is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.

        Returns:
            The PyDough QDAG object for the qualified expression node.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        answer: PyDoughQDAG = self.qualify_node(unqualified, context, children, True)
        if not isinstance(answer, PyDoughExpressionQDAG):
            raise PyDoughUnqualifiedException(
                f"Expected an expression, but received a collection: {answer}"
            )
        return answer

    def qualify_node(
        self,
        unqualified: UnqualifiedNode,
        context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
        is_child: bool,
    ) -> PyDoughQDAG:
        """
        Transforms an UnqualifiedNode into a PyDoughQDAG node that can be either
        a collection or an expression.

        Args:
            `unqualified`: the UnqualifiedNode instance to be transformed.
            `context`: the collection QDAG whose context the expression is being
            evaluated within.
            `children`: the list where collection nodes that must be derived
            as children of `context` should be appended.
            `is_child`: whether the collection is being qualified as a child
            of a child operator context, such as CALCULATE or PARTITION.

        Returns:
            The PyDough QDAG object for the qualified node. The result can be either
            an expression or a collection.

        Raises:
            `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
            goes wrong during the qualification process, e.g. a term cannot be
            qualified or is not recognized.
        """
        unqualified_str: str = display_raw(unqualified)
        lookup: PyDoughQDAG | None = self.lookup_if_already_qualified(
            unqualified_str, context
        )
        if lookup is not None:
            return lookup
        answer: PyDoughQDAG
        match unqualified:
            case UnqualifiedRoot():
                # Special case: when the root has been reached, it is assumed
                # to refer to the context variable that was passed in.
                answer = context
            case UnqualifiedAccess():
                answer = self.qualify_access(unqualified, context, children, is_child)
            case UnqualifiedCalculate():
                answer = self.qualify_calculate(unqualified, context, is_child)
            case UnqualifiedWhere():
                answer = self.qualify_where(unqualified, context, is_child)
            case UnqualifiedOrderBy():
                answer = self.qualify_order_by(unqualified, context, is_child)
            case UnqualifiedTopK():
                answer = self.qualify_top_k(unqualified, context, is_child)
            case UnqualifiedPartition():
                answer = self.qualify_partition(unqualified, context, is_child)
            case UnqualifiedLiteral():
                answer = self.qualify_literal(unqualified)
            case UnqualifiedOperation():
                answer = self.qualify_operation(unqualified, context, children)
            case UnqualifiedWindow():
                answer = self.qualify_window(unqualified, context, children)
            case UnqualifiedBinaryOperation():
                answer = self.qualify_binary_operation(unqualified, context, children)
            case UnqualifiedCollation():
                answer = self.qualify_collation(unqualified, context, children)
            case _:
                raise PyDoughUnqualifiedException(
                    f"Cannot qualify {unqualified.__class__.__name__}: {unqualified!r}"
                )
        # Store the answer for cached lookup, then return it.
        self.add_definition(unqualified_str, context, answer)
        return answer


def qualify_node(unqualified: UnqualifiedNode, graph: GraphMetadata) -> PyDoughQDAG:
    """
    Transforms an UnqualifiedNode into a qualified node.

    Args:
        `unqualified`: the UnqualifiedNode instance to be transformed.
        `graph`: the metadata for the graph that the PyDough computations
        are occurring within.

    Returns:
        The PyDough QDAG object for the qualified node. The result can be either
        an expression or a collection.

    Raises:
        `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
        goes wrong during the qualification process, e.g. a term cannot be
        qualified or is not recognized.
    """
    qual: Qualifier = Qualifier(graph)
    return qual.qualify_node(
        unqualified, qual.builder.build_global_context(), [], False
    )


def qualify_term(
    collection: PyDoughCollectionQDAG, term: UnqualifiedNode, graph: GraphMetadata
) -> tuple[list[PyDoughCollectionQDAG], PyDoughQDAG]:
    """
    Transforms an UnqualifiedNode into a qualified node within the context of
    a collection, e.g. to learn about a subcollection or expression of a
    qualified PyDough collection..

    Args:
        `collection`: the qualified collection instance corresponding to the
        context in which the term is being qualified.
        `term`: the UnqualifiedNode instance to be transformed into a qualified
        node within the context of `collection`.
        `graph`: the metadata for the graph that the PyDough computations
        are occurring within.

    Returns:
        A tuple where the second entry is the PyDough QDAG object for the
        qualified term. The result can be either an expression or a collection.
        The first entry is a list of any additional children of `collection`
        that must be derived in order to evaluate `term`.

    Raises:
        `PyDoughUnqualifiedException` or `PyDoughQDAGException` if something
        goes wrong during the qualification process, e.g. a term cannot be
        qualified or is not recognized.
    """
    qual: Qualifier = Qualifier(graph)
    children: list[PyDoughCollectionQDAG] = []
    return children, qual.qualify_node(term, collection, children, True)
