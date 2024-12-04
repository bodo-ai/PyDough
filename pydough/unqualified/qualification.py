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
    ChildReferenceExpression,
    CollationExpression,
    GlobalContext,
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
        r"Cannot qualify (\S*) as an expression"
    )

    def __init__(self, graph: GraphMetadata):
        self._graph: GraphMetadata = graph
        self._builder: AstNodeBuilder = AstNodeBuilder(graph)
        self._memo: dict[tuple[str, PyDoughCollectionAST], PyDoughAST] = {}

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
        The builder used by the qualifier to create AST nodes.
        """
        return self._builder

    def lookup_if_already_qualified(
        self,
        unqualified_str: str,
        context: PyDoughCollectionAST,
    ) -> PyDoughAST | None:
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
        context: PyDoughCollectionAST,
        qualified_node: PyDoughAST,
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

    def qualify_literal(self, unqualified: UnqualifiedLiteral) -> PyDoughExpressionAST:
        """
        Transforms an `UnqualifiedLiteral` into a PyDoughExpressionAST node.

        Args:
            `unqualified`: the UnqualifiedLiteral instance to be transformed.
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
        value: object = unqualified._parcel[0]
        data_type: PyDoughType = unqualified._parcel[1]
        return self.builder.build_literal(value, data_type)

    def qualify_operation(
        self,
        unqualified: UnqualifiedOperation,
        context: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ) -> PyDoughExpressionAST:
        """
        Transforms an `UnqualifiedOperation` into a PyDoughExpressionAST node.

        Args:
            `unqualified`: the UnqualifiedOperation instance to be transformed.
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
        operation: str = unqualified._parcel[0]
        unqualified_operands: MutableSequence[UnqualifiedNode] = unqualified._parcel[1]
        qualified_operands: MutableSequence[PyDoughAST] = []
        # Iterate across every operand to generate its qualified variant.
        # First, attempt to qualify it as an expression (the common case), but
        # if that fails specifically because the result would be a collection,
        # then attempt to qualify it as a collection.
        for node in unqualified_operands:
            try:
                qualified_operands.append(
                    self.qualify_expression(node, context, children)
                )
            except PyDoughUnqualifiedException as e:
                if self.not_expression_pattern.match(str(e)):
                    child_collection: PyDoughCollectionAST = self.qualify_collection(
                        node, context, True
                    )
                    # If the operand could be qualified as a collection, then
                    # add it to the children list (if not already present) and
                    # use a child reference collection as the argument.
                    ref_num: int
                    if child_collection in children:
                        ref_num = children.index(child_collection)
                    else:
                        ref_num = len(children)
                        children.append(child_collection)
                    child_collection_ref: PyDoughCollectionAST = (
                        self.builder.build_child_reference_collection(
                            context, children, ref_num
                        )
                    )
                    qualified_operands.append(child_collection_ref)
                else:
                    raise e
        return self.builder.build_expression_function_call(
            operation, qualified_operands
        )

    def qualify_binary_operation(
        self,
        unqualified: UnqualifiedBinaryOperation,
        context: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ) -> PyDoughExpressionAST:
        """
        Transforms an `UnqualifiedBinaryOperation` into a PyDoughExpressionAST node.

        Args:
            `unqualified`: the UnqualifiedBinaryOperation instance to be transformed.
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
        qualified_lhs: PyDoughExpressionAST = self.qualify_expression(
            unqualified_lhs, context, children
        )
        qualified_rhs: PyDoughExpressionAST = self.qualify_expression(
            unqualified_rhs, context, children
        )
        return self.builder.build_expression_function_call(
            operation, [qualified_lhs, qualified_rhs]
        )

    def qualify_collation(
        self,
        unqualified: UnqualifiedCollation,
        context: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ) -> PyDoughExpressionAST:
        """
        Transforms an `UnqualifiedCollation` into a PyDoughExpressionAST node.

        Args:
            `unqualified`: the UnqualifiedCollation instance to be transformed.
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
        unqualified_expr: UnqualifiedNode = unqualified._parcel[0]
        asc: bool = unqualified._parcel[1]
        na_last: bool = unqualified._parcel[2]
        # Qualify the underlying expression, then wrap it in a collation.
        qualified_expr: PyDoughExpressionAST = self.qualify_expression(
            unqualified_expr, context, children
        )
        return CollationExpression(qualified_expr, asc, na_last)

    def qualify_expression_access(
        self,
        unqualified: UnqualifiedAccess,
        context: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ) -> PyDoughExpressionAST:
        """
        Transforms an `UnqualifiedAccess` into a PyDoughExpressionAST node.

        Args:
            `unqualified`: the UnqualifiedAccess instance to be transformed.
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
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        name: str = unqualified._parcel[1]
        if isinstance(unqualified_parent, UnqualifiedBack):
            # If the parent is an `UnqualifiedBack`, it means that this is an
            # expression in the form "BACK(n).term_name".
            levels: int = unqualified_parent._parcel[0]
            return self.builder.build_back_reference_expression(context, name, levels)
        else:
            # First, qualify the parent and confirm that it has an expression
            # property with the desired name.
            qualified_parent: PyDoughCollectionAST = self.qualify_collection(
                unqualified_parent, context, True
            )
            term: PyDoughAST = qualified_parent.get_term(name)
            if not isinstance(term, PyDoughExpressionAST):
                raise PyDoughUnqualifiedException(
                    f"Cannot qualify {unqualified.__class__.__name__} as an expression: {unqualified!r}"
                )
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

    def qualify_expression(
        self,
        unqualified: UnqualifiedNode,
        context: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
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
        # First, attempt to lookup a previously cached answer, if one exists
        unqualified_str: str = str(unqualified)
        lookup: PyDoughAST | None = self.lookup_if_already_qualified(
            unqualified_str, context
        )
        if lookup is not None:
            if not isinstance(lookup, PyDoughExpressionAST):
                raise PyDoughUnqualifiedException(
                    f"Cannot qualify {unqualified.__class__.__name__} as an expression: {unqualified!r}"
                )
            return lookup

        # Dispatch onto the correct handler logic based on the type of
        # unqualified node
        answer: PyDoughExpressionAST
        match unqualified:
            case UnqualifiedLiteral():
                answer = self.qualify_literal(unqualified)
            case UnqualifiedOperation():
                answer = self.qualify_operation(unqualified, context, children)
            case UnqualifiedBinaryOperation():
                answer = self.qualify_binary_operation(unqualified, context, children)
            case UnqualifiedCollation():
                answer = self.qualify_collation(unqualified, context, children)
            case UnqualifiedAccess():
                answer = self.qualify_expression_access(unqualified, context, children)
            case _:
                raise PyDoughUnqualifiedException(
                    f"Cannot qualify {unqualified.__class__.__name__} as an expression: {unqualified!r}"
                )
        # Store the answer for cached lookup, then return it.
        self.add_definition(unqualified_str, context, answer)
        return answer

    def qualify_collection_access(
        self,
        unqualified: UnqualifiedAccess,
        context: PyDoughCollectionAST,
        is_child: bool,
    ) -> PyDoughCollectionAST:
        """
        Transforms an `UnqualifiedAccess` into a PyDoughCollectionAST node.

        Args:
            `unqualified`: the UnqualifiedAccess instance to be transformed.
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
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        name: str = unqualified._parcel[1]
        if isinstance(unqualified_parent, UnqualifiedBack):
            # If the parent is an `UnqualifiedBack`, it means that this is a
            # collection in the form "BACK(n).term_name".
            levels: int = unqualified_parent._parcel[0]
            return self.builder.build_back_reference_collection(context, name, levels)
        else:
            # First, qualify the parent collection.
            qualified_parent: PyDoughCollectionAST = self.qualify_collection(
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
                # Otherwise, access the child collection from the qualified
                # parent collection
                answer: PyDoughCollectionAST = self.builder.build_child_access(
                    name, qualified_parent
                )
                if isinstance(unqualified_parent, UnqualifiedRoot) and is_child:
                    answer = ChildOperatorChildAccess(answer)
                return answer

    def qualify_calc(
        self,
        unqualified: UnqualifiedCalc,
        context: PyDoughCollectionAST,
        is_child: bool,
    ) -> PyDoughCollectionAST:
        """
        Transforms an `UnqualifiedCalc` into a PyDoughCollectionAST node.

        Args:
            `unqualified`: the UnqualifiedCalc instance to be transformed.
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
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        unqualified_terms: MutableSequence[tuple[str, UnqualifiedNode]] = (
            unqualified._parcel[1]
        )
        qualified_parent: PyDoughCollectionAST = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        # Qualify all of the CALC terms, storing the children built along
        # the way.
        children: MutableSequence[PyDoughCollectionAST] = []
        qualified_terms: MutableSequence[tuple[str, PyDoughExpressionAST]] = []
        for name, term in unqualified_terms:
            qualified_term = self.qualify_expression(term, qualified_parent, children)
            qualified_terms.append((name, qualified_term))
        # Use the qualified children & terms to create a new CALC node.
        calc: Calc = self.builder.build_calc(qualified_parent, children)
        return calc.with_terms(qualified_terms)

    def qualify_where(
        self,
        unqualified: UnqualifiedWhere,
        context: PyDoughCollectionAST,
        is_child: bool,
    ) -> PyDoughCollectionAST:
        """
        Transforms an `UnqualifiedWhere` into a PyDoughCollectionAST node.

        Args:
            `unqualified`: the UnqualifiedWhere instance to be transformed.
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
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        unqualified_cond: UnqualifiedNode = unqualified._parcel[1]
        qualified_parent: PyDoughCollectionAST = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        # Qualify the condition of the WHERE clause, storing the children
        # built along the way.
        children: MutableSequence[PyDoughCollectionAST] = []
        qualified_cond = self.qualify_expression(
            unqualified_cond, qualified_parent, children
        )
        # Use the qualified children & condition to create a new WHERE node.
        where: Where = self.builder.build_where(qualified_parent, children)
        return where.with_condition(qualified_cond)

    def qualify_order_by(
        self,
        unqualified: UnqualifiedOrderBy,
        context: PyDoughCollectionAST,
        is_child: bool,
    ) -> PyDoughCollectionAST:
        """
        Transforms an `UnqualifiedOrderBy` into a PyDoughCollectionAST node.

        Args:
            `unqualified`: the UnqualifiedOrderBy instance to be transformed.
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
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        unqualified_terms: MutableSequence[UnqualifiedNode] = unqualified._parcel[1]
        qualified_parent: PyDoughCollectionAST = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        # Qualify all of the collation terms, storing the children built along
        # the way.
        children: MutableSequence[PyDoughCollectionAST] = []
        qualified_collations: list[CollationExpression] = []
        for term in unqualified_terms:
            qualified_term: PyDoughExpressionAST = self.qualify_expression(
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
        context: PyDoughCollectionAST,
        is_child: bool,
    ) -> PyDoughCollectionAST:
        """
        Transforms an `UnqualifiedTopK` into a PyDoughCollectionAST node.

        Args:
            `unqualified`: the UnqualifiedTopK instance to be transformed.
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
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        records_to_keep: int = unqualified._parcel[1]
        # TODO: add ability to infer the "by" clause from a predecessor
        assert (
            unqualified._parcel[2] is not None
        ), "TopK does not currently support an implied 'by' clause."
        unqualified_terms: MutableSequence[UnqualifiedNode] = unqualified._parcel[2]
        qualified_parent: PyDoughCollectionAST = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        # Qualify all of the collation terms, storing the children built along
        # the way.
        children: MutableSequence[PyDoughCollectionAST] = []
        qualified_collations: list[CollationExpression] = []
        for term in unqualified_terms:
            qualified_term: PyDoughExpressionAST = self.qualify_expression(
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
        context: PyDoughCollectionAST,
        is_child: bool,
    ) -> PyDoughCollectionAST:
        """
        Transforms an `UnqualifiedPartition` into a PyDoughCollectionAST node.

        Args:
            `unqualified`: the UnqualifiedPartition instance to be transformed.
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
        unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
        unqualified_child: UnqualifiedNode = unqualified._parcel[1]
        child_name: str = unqualified._parcel[2]
        unqualified_terms: MutableSequence[UnqualifiedNode] = unqualified._parcel[3]
        # Qualify all both the parent collection and the child that is being
        # partitioned, using the qualified parent as the context for the
        # child.
        qualified_parent: PyDoughCollectionAST = self.qualify_collection(
            unqualified_parent, context, is_child
        )
        qualified_child: PyDoughCollectionAST = self.qualify_collection(
            unqualified_child, qualified_parent, True
        )
        # Qualify all of the partitioning keys (which, for now, can only be
        # references to expressions in the child), storing the children built
        # along the way (which should just be the child input).
        child_references: list[ChildReferenceExpression] = []
        children: MutableSequence[PyDoughCollectionAST] = []
        for term in unqualified_terms:
            qualified_term: PyDoughExpressionAST = self.qualify_expression(
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
        return partition.with_keys(child_references)

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
        PyDoughUnqualifiedException(
            f"Cannot qualify {unqualified.__class__.__name__} as a collection: {unqualified!r}"
        )
        if lookup is not None:
            if not isinstance(lookup, PyDoughCollectionAST):
                raise PyDoughUnqualifiedException(
                    f"Cannot qualify {unqualified.__class__.__name__} as a collection: {unqualified!r}"
                )
            return lookup
        answer: PyDoughCollectionAST
        match unqualified:
            case UnqualifiedRoot():
                # Special case: when the root has been reached, it is assumed
                # to refer to the context variable that was passed in.
                answer = context
            case UnqualifiedAccess():
                answer = self.qualify_collection_access(unqualified, context, is_child)
            case UnqualifiedCalc():
                answer = self.qualify_calc(unqualified, context, is_child)
            case UnqualifiedWhere():
                answer = self.qualify_where(unqualified, context, is_child)
            case UnqualifiedOrderBy():
                answer = self.qualify_order_by(unqualified, context, is_child)
            case UnqualifiedTopK():
                answer = self.qualify_top_k(unqualified, context, is_child)
            case UnqualifiedPartition():
                answer = self.qualify_partition(unqualified, context, is_child)
            case _:
                raise PyDoughUnqualifiedException(
                    f"Cannot qualify {unqualified.__class__.__name__} as a collection: {unqualified!r}"
                )
        # Store the answer for cached lookup, then return it.
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
