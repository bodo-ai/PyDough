"""
TODO: add file-level docstring
"""

__all__ = ["qualify_node"]

from functools import cache

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import (
    AstNodeBuilder,
    Calc,
    PyDoughAST,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
)
from pydough.types import PyDoughType

from .errors import PyDoughUnqualifiedException
from .unqualified_node import (
    UnqualifiedAccess,
    UnqualifiedCalc,
    UnqualifiedLiteral,
    UnqualifiedNode,
    UnqualifiedRoot,
)


class Qualifier:
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
        if lookup is not None:
            assert isinstance(
                lookup, PyDoughExpressionAST
            ), f"Expected qualified answer to be an expression, received {lookup.__class__.__name__}"
            return lookup
        answer: PyDoughExpressionAST
        name: str
        match unqualified:
            case UnqualifiedLiteral():
                value: object = unqualified._parcel[0]
                data_type: PyDoughType = unqualified._parcel[1]
                answer = self.builder.build_literal(value, data_type)
            case UnqualifiedAccess():
                unqualified_parent: UnqualifiedNode = unqualified._parcel[0]
                qualified_parent: PyDoughCollectionAST = self.qualify_collection(
                    unqualified_parent, context
                )
                name = unqualified._parcel[1]
                ref_num: int
                if qualified_parent in children:
                    ref_num = children.index(qualified_parent)
                else:
                    ref_num = len(children)
                    children.append(qualified_parent)
                answer = self.builder.build_child_reference(children, ref_num, name)
            case _:
                raise PyDoughUnqualifiedException(f"Cannot qualify {unqualified!r}")
        self.add_definition(unqualified_str, context, answer)
        return answer

    @cache
    def qualify_collection(
        self, unqualified: UnqualifiedNode, context: PyDoughCollectionAST
    ) -> PyDoughCollectionAST:
        """
        Transforms an `UnqualifiedNode` into a PyDoughCollectionAST node.

        Args:
            `unqualified`: the UnqualifiedNode instance to be transformed.
            `builder`: a builder object used to create new qualified nodes.
            `context`: the collection AST whose context the collection is being
            evaluated within.

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
        if lookup is not None:
            assert isinstance(
                lookup, PyDoughCollectionAST
            ), f"Expected qualified answer to be a collection, received {lookup.__class__.__name__}"
            return lookup
        answer: PyDoughCollectionAST
        name: str
        unqualified_parent: UnqualifiedNode
        qualified_parent: PyDoughCollectionAST
        children: list[PyDoughCollectionAST]
        match unqualified:
            case UnqualifiedAccess():
                unqualified_parent = unqualified._parcel[0]
                name = unqualified._parcel[1]
                qualified_parent = self.qualify_collection(unqualified_parent)
                answer = self.builder.build_child_access(name, qualified_parent)
            case UnqualifiedRoot():
                answer = context
            case UnqualifiedCalc():
                unqualified_parent = unqualified._parcel[0]
                unqualified_terms: list[tuple[str, UnqualifiedNode]] = (
                    unqualified._parcel[1]
                )
                qualified_parent = self.qualify_collection(unqualified_parent)
                children = []
                terms: list[tuple[str, PyDoughExpressionAST]] = []
                for name, term in unqualified_terms:
                    qualified_term: PyDoughExpressionAST = self.qualify_expression(
                        term, qualified_parent, children
                    )
                    terms.append((name, qualified_term))
                calc: Calc = self.builder.build_calc(qualified_parent, children)
                answer = calc.with_terms(terms)
            case _:
                raise PyDoughUnqualifiedException(f"Cannot qualify {unqualified!r}")
        self.add_definition(unqualified_str, context, answer)
        return answer


def qualify_node(unqualified: UnqualifiedNode, graph: GraphMetadata) -> PyDoughAST:
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
    return qual.qualify_collection(unqualified, qual.builder.build_global_context())
