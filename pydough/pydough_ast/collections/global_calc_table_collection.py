"""
TODO: add file-level docstring
"""

__all__ = ["GlobalCalcTableCollection"]


from .table_collection import TableCollection
from .collection_tree_form import CollectionTreeForm


class GlobalCalcTableCollection(TableCollection):
    """
    Special wrapper around a TableCollection instance that denotes it as an
    immediate child of a global CALC node, for the purposes of stringification.
    """

    def __init__(
        self,
        collection: TableCollection,
        is_last: bool,
    ):
        super().__init__(collection.collection)
        self._is_last: bool = is_last

    @property
    def is_last(self) -> bool:
        """
        Whether this is the last child of the parent Calc.
        """
        return self._is_last

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = CollectionTreeForm(
            "CalcSubCollection",
            0,
            has_predecessor=True,
            has_children=True,
            has_successor=not self.is_last,
        )
        return CollectionTreeForm(
            f"TableCollection[{self.to_string()}]",
            predecessor.depth + 1,
            predecessor=predecessor,
        )
