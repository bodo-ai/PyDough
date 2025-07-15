from pydough.qdag import PyDoughCollectionQDAG
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.qdag.expressions.collation_expression import CollationExpression
from pydough.qdag.expressions.reference import Reference

from .child_access import ChildAccess
from .collection_tree_form import CollectionTreeForm
from .user_collections import PyDoughUserGeneratedCollection


# or should it be CollectionAccess?
class PyDoughUserGeneratedCollectionQDag(ChildAccess):
    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
        collection: PyDoughUserGeneratedCollection,
    ):
        assert ancestor is not None
        super().__init__(ancestor)
        self._collection = collection

    def clone_with_parent(
        self, new_ancestor: PyDoughCollectionQDAG
    ) -> "PyDoughUserGeneratedCollectionQDag":
        """
        Copies `self` but with a new ancestor node that presumably has the
        original ancestor in its predecessor chain.

        Args:
            `new_ancestor`: the node to use as the new parent of the clone.

        Returns:
            The cloned version of `self`.
        """
        return PyDoughUserGeneratedCollectionQDag(new_ancestor, self._collection)

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        # if self.ancestor_context is not None:
        #     return CollectionTreeForm(
        #         self.tree_item_string,
        #         0,
        #         has_predecessor=True,
        #     )
        # else:
        return CollectionTreeForm(self.tree_item_string, 0)

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        # if self.ancestor_context is not None:
        #     predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form(
        #         is_last
        #     )
        #     predecessor.has_children = True
        #     tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
        #     tree_form.depth = predecessor.depth + 1
        #     tree_form.predecessor = predecessor
        #     return tree_form
        # else:
        return self.to_tree_form_isolated(is_last)

    @property
    def collection(self) -> PyDoughUserGeneratedCollection:
        """
        The metadata for the table that is being referenced by the collection
        node.
        """
        return self._collection

    @property
    def name(self) -> str:
        return self.collection.name

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG | None:
        return None

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    @property
    def calc_terms(self) -> set[str]:
        return set(self.collection.columns)

    @property
    def all_terms(self) -> set[str]:
        """
        The set of expression/subcollection names accessible by the context.
        """
        return self.calc_terms

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self._ancestor.ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        if self._ancestor:
            return self._ancestor.inherited_downstreamed_terms
        else:
            return set()

    def is_singular(self, context: "PyDoughCollectionQDAG") -> bool:
        return False

    def get_term(self, term_name: str) -> PyDoughQDAG:
        if term_name not in self.collection.columns:
            raise PyDoughQDAGException(self.name_mismatch_error(term_name))

        return Reference(self._ancestor, term_name)

    def get_expression_position(self, expr_name: str) -> int:
        raise PyDoughQDAGException(f"Cannot call get_expression_position on {self!r}")

    @property
    def unique_terms(self) -> list[str]:
        return self.collection.columns

    @property
    def standalone_string(self) -> str:
        """
        Returns a string representation of the collection in a standalone form.
        This is used for debugging and logging purposes.
        """
        return f"UserGeneratedCollection[{self.name}, {', '.join(self.collection.columns)}]"

    @property
    def key(self) -> str:
        return f"USER_GENERATED_COLLECTION-{self.name}"

    def to_string(self) -> str:
        # Stringify as "name(column_name)
        return f"{self.name}({', '.join(self.collection.columns)})"

    @property
    def tree_item_string(self) -> str:
        return f"UserGeneratedCollection[{self.name}: {', '.join(self.collection.columns)}]"
