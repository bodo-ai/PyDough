"""
TODO: add file-level docstring
"""

__all__ = ["CalcSubCollection"]


from .sub_collection import SubCollection
from .hidden_back_reference_collection import HiddenBackReferenceCollection
from .collection_tree_form import CollectionTreeForm


class CalcSubCollection(SubCollection):
    """
    Special wrapper around a SubCollection instance that denotes it as an
    immediate child of a CALC node, for the purposes of stringification.
    """

    def __init__(
        self,
        subcollection: SubCollection,
        is_last: bool,
    ):
        super().__init__(subcollection.parent, subcollection.subcollection_property)
        self._subcollection: SubCollection = subcollection
        self._is_last: bool = is_last

    @property
    def is_last(self) -> bool:
        """
        Whether this is the last child of the parent Calc.
        """
        return self._is_last

    @property
    def subcollection(self) -> SubCollection:
        """
        The SubCollection node that is being wrapped.
        """
        return self._subcollection

    def to_string(self) -> str:
        # Does not include the parent since this exists within the context
        # of a CALC node.
        if isinstance(self.subcollection, HiddenBackReferenceCollection):
            return self.subcollection.alias
        else:
            return self.subcollection_property.name

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = CollectionTreeForm(
            "CalcSubCollection",
            0,
            has_predecessor=True,
            has_children=True,
            has_successor=not self.is_last,
        )
        return CollectionTreeForm(
            f"SubCollection[{self.to_string()}]",
            predecessor.depth + 1,
            predecessor=predecessor,
        )
