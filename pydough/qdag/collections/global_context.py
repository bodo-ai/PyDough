"""
Definition of PyDough QDAG collection type for the basic context that has one
record, no expressions, and access to all the top-level collections in the
graph.
"""

__all__ = ["TableCollection"]


from pydough.errors import PyDoughQDAGException
from pydough.metadata import (
    CollectionMetadata,
    GraphMetadata,
)
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions import BackReferenceExpression, CollationExpression

from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm
from .table_collection import TableCollection


class GlobalContext(PyDoughCollectionQDAG):
    """
    The QDAG node implementation class representing the graph-level context
    containing all of the collections.
    """

    def __init__(
        self, graph: GraphMetadata, ancestor: PyDoughCollectionQDAG | None = None
    ):
        self._ancestor: PyDoughCollectionQDAG | None = ancestor
        self._graph = graph
        self._collections: dict[str, PyDoughCollectionQDAG] = {}
        self._ancestral_mapping: dict[str, int] = {}
        self._all_terms: set[str] = set()
        # If this collection has an ancestor, inherit its ancestors
        # and update depths, to preserve sub-collection context.
        # This ensures that downstream operations have the correct hierarchy.
        # Also, check for name conflicts with existing collections
        if ancestor is not None:
            for name, level in ancestor.ancestral_mapping.items():
                if name in graph.get_collection_names():
                    raise PyDoughQDAGException(
                        f"Name {name!r} conflicts with a collection name in the graph {graph.name!r}"
                    )
                else:
                    self._all_terms.add(name)
                    self._ancestral_mapping[name] = level + 1
        for collection_name in graph.get_collection_names():
            meta = graph.get_collection(collection_name)
            assert isinstance(meta, CollectionMetadata)
            self._collections[collection_name] = TableCollection(meta, self)
            self._all_terms.add(collection_name)

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
        return self._ancestor

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG | None:
        return None

    @property
    def calc_terms(self) -> set[str]:
        # A global context does not have any calc terms
        return set()

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self._ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        if self._ancestor:
            return self._ancestor.inherited_downstreamed_terms
        else:
            return set()

    @property
    def all_terms(self) -> set[str]:
        return self._all_terms

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    @property
    def unique_terms(self) -> list[str]:
        return []

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        return (
            self.ancestor_context is None
            or self.ancestor_context.starting_predecessor == context
            or self.ancestor_context.is_singular(context)
        )

    def get_expression_position(self, expr_name: str) -> int:
        raise NotImplementedError(f"Cannot call get_expression_position on {self!r}")

    def get_term(self, term_name: str) -> PyDoughQDAG:
        self.verify_term_exists(term_name)
        if term_name in self.collections:
            return self.collections[term_name]
        else:
            return BackReferenceExpression(
                self, term_name, self.ancestral_mapping[term_name]
            )

    @property
    def standalone_string(self) -> str:
        return self.graph.name

    def to_string(self) -> str:
        if self.ancestor_context is not None:
            return f"{self.ancestor_context.to_string()}.{self.standalone_string}"
        return self.standalone_string

    @property
    def tree_item_string(self) -> str:
        return self.standalone_string

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        if self.ancestor_context is not None:
            return CollectionTreeForm(
                self.tree_item_string,
                0,
                has_predecessor=True,
            )
        else:
            return CollectionTreeForm(self.tree_item_string, 0)

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        if self.ancestor_context is not None:
            predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form(
                is_last
            )
            predecessor.has_children = True
            tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
            tree_form.depth = predecessor.depth + 1
            tree_form.predecessor = predecessor
            return tree_form
        else:
            return self.to_tree_form_isolated(is_last)

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, GlobalContext)
            and self.graph == other.graph
            and self.ancestor_context == other.ancestor_context
        )
