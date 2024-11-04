"""
TODO: add file-level docstring
"""

__all__ = ["TableCollection"]


from typing import Dict, Set

from pydough.metadata import (
    GraphMetadata,
)
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from .collection_ast import PyDoughCollectionAST
from .table_collection import TableCollection
from .collection_tree_form import CollectionTreeForm


class GlobalContext(PyDoughCollectionAST):
    """
    The AST node implementation class representing the graph-level context
    containing all of the collections.
    """

    def __init__(self, graph: GraphMetadata):
        self._graph = graph
        self._collections: Dict[str, PyDoughCollectionAST] = {}
        for collection_name in graph.get_collection_names():
            self._collections[collection_name] = TableCollection(
                graph.get_collection(collection_name), self
            )

    @property
    def graph(self) -> GraphMetadata:
        """
        The metadata for the graph that the context refers to.
        """
        return self._graph

    @property
    def collections(self) -> PyDoughCollectionAST:
        """
        The collections that the context has access to.
        """
        return self._collections

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return None

    @property
    def preceding_context(self) -> PyDoughCollectionAST | None:
        return None

    @property
    def calc_terms(self) -> Set[str]:
        # A global context does not have any calc terms
        return set()

    @property
    def all_terms(self) -> Set[str]:
        return set(self.collections)

    def get_expression_position(self, expr_name: str) -> int:
        raise PyDoughASTException(f"Cannot call get_expression_position on {self!r}")

    def get_term(self, term_name: str) -> PyDoughAST:
        if term_name not in self.collections:
            raise PyDoughASTException(
                f"Unrecognized term of {self.graph.error_name}: {term_name!r}"
            )
        return self.collections[term_name]

    def to_string(self) -> str:
        return self.graph.name

    def to_tree_form(self) -> CollectionTreeForm:
        return CollectionTreeForm(self.to_string(), 0)

    def equals(self, other: "GlobalContext") -> bool:
        return super().equals(other) and self.graph == other.graph
