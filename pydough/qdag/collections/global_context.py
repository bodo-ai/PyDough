"""
Definition of PyDough QDAG collection type for the basic context that has one
record, no expressions, and access to all the top-level collections in the
graph.
"""

__all__ = ["TableCollection"]


from pydough.metadata import (
    CollectionMetadata,
    GraphMetadata,
)
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.qdag.expressions import CollationExpression

from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm
from .table_collection import TableCollection


class GlobalContext(PyDoughCollectionQDAG):
    """
    The QDAG node implementation class representing the graph-level context
    containing all of the collections.
    """

    def __init__(self, graph: GraphMetadata):
        self._graph = graph
        self._collections: dict[str, PyDoughCollectionQDAG] = {}
        for collection_name in graph.get_collection_names():
            meta = graph.get_collection(collection_name)
            assert isinstance(meta, CollectionMetadata)
            self._collections[collection_name] = TableCollection(meta, self)

    @property
    def graph(self) -> GraphMetadata:
        """
        The metadata for the graph that the context refers to.
        """
        return self._graph

    @property
    def collections(self) -> dict[str, PyDoughCollectionQDAG]:
        """
        The collections that the context has access to.
        """
        return self._collections

    @property
    def name(self) -> str:
        return self.graph.name

    @property
    def key(self) -> str:
        return f"{self.graph.name}"

    @property
    def ancestor_context(self) -> PyDoughCollectionQDAG | None:
        return None

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG | None:
        return None

    @property
    def calc_terms(self) -> set[str]:
        # A global context does not have any calc terms
        return set()

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        # A global context does not have any ancestral terms
        return {}

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        # A global context does not have any inherited downstreamed terms
        return set()

    @property
    def all_terms(self) -> set[str]:
        return set(self.collections)

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    @property
    def unique_terms(self) -> list[str]:
        return []

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        raise PyDoughQDAGException(f"Cannot call is_singular on {self!r}")

    def get_expression_position(self, expr_name: str) -> int:
        raise PyDoughQDAGException(f"Cannot call get_expression_position on {self!r}")

    def get_term(self, term_name: str) -> PyDoughQDAG:
        if term_name not in self.collections:
            raise PyDoughQDAGException(
                f"Unrecognized term of {self.graph.error_name}: {term_name!r}"
            )
        return self.collections[term_name]

    @property
    def standalone_string(self) -> str:
        return self.graph.name

    def to_string(self) -> str:
        return self.standalone_string

    @property
    def tree_item_string(self) -> str:
        return self.standalone_string

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        return CollectionTreeForm(self.to_string(), 0)

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        return self.to_tree_form_isolated(is_last)

    def equals(self, other: object) -> bool:
        return isinstance(other, GlobalContext) and self.graph == other.graph
