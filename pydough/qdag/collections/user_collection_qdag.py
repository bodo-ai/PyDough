from functools import cache

from pydough.errors import PyDoughQDAGException
from pydough.qdag import PyDoughCollectionQDAG
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions.back_reference_expression import BackReferenceExpression
from pydough.qdag.expressions.reference import Reference
from pydough.types import NumericType
from pydough.user_collections.user_collections import PyDoughUserGeneratedCollection

from .child_access import ChildAccess


class PyDoughUserGeneratedCollectionQDag(ChildAccess):
    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
        collection: PyDoughUserGeneratedCollection,
    ):
        assert ancestor is not None
        super().__init__(ancestor)
        self._collection: PyDoughUserGeneratedCollection = collection
        self._all_property_names: set[str] = set()
        self._ancestral_mapping: dict[str, int] = {
            name: level + 1 for name, level in ancestor.ancestral_mapping.items()
        }
        self._all_property_names.update(self._ancestral_mapping)
        self._all_property_names.update(self.calc_terms)

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
    def calc_terms(self) -> set[str]:
        return set(self.collection.columns)

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self._ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        return self.ancestor_context.inherited_downstreamed_terms

    @cache
    def get_term(self, term_name: str) -> PyDoughQDAG:
        # Special handling of terms down-streamed
        if term_name in self.ancestral_mapping:
            # Verify that the ancestor name is not also a name in the current
            # context.
            if term_name in self.calc_terms:
                raise PyDoughQDAGException(
                    f"Cannot have term name {term_name!r} used in an ancestor of collection {self!r}"
                )
            # Create a back-reference to the ancestor term.
            return BackReferenceExpression(
                self, term_name, self.ancestral_mapping[term_name]
            )

        if term_name in self.inherited_downstreamed_terms:
            context: PyDoughCollectionQDAG = self
            while term_name not in context.all_terms:
                if context is self:
                    context = self.ancestor_context
                else:
                    assert context.ancestor_context is not None
                    context = context.ancestor_context
            return Reference(
                context, term_name, context.get_expr(term_name).pydough_type
            )

        if term_name not in self.all_terms:
            raise PyDoughQDAGException(self.name_mismatch_error(term_name))

        return Reference(self, term_name, NumericType())

    @property
    def all_terms(self) -> set[str]:
        """
        The set of expression/subcollection names accessible by the context.
        """
        return self._all_property_names

    def is_singular(self, context: "PyDoughCollectionQDAG") -> bool:
        return False

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.calc_terms:
            raise PyDoughQDAGException(
                f"Unrecognized User Collection term: {expr_name!r}"
            )
        return self.collection.get_expression_position(expr_name)

    @property
    def unique_terms(self) -> list[str]:
        return self.collection.unique_column_names

    @property
    def standalone_string(self) -> str:
        return self.to_string()

    @property
    def key(self) -> str:
        return f"USER_GENERATED_COLLECTION-{self.name}"

    def to_string(self) -> str:
        return f"UserCollection[{self.collection.to_string()}]"

    @property
    def tree_item_string(self) -> str:
        return self.collection.to_string()
