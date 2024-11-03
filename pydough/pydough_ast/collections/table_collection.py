"""
TODO: add file-level docstring
"""

__all__ = ["TableCollection"]


from typing import Dict, List, Tuple, Set

from pydough.metadata import (
    CollectionMetadata,
    PropertyMetadata,
    CompoundRelationshipMetadata,
)
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import ColumnProperty
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


class TableCollection(PyDoughCollectionAST):
    """
    The AST node implementation class representing a table collection accessed
    as a root.
    """

    def __init__(self, collection: CollectionMetadata):
        self._collection: CollectionMetadata = collection
        self._properties: Dict[str, Tuple[int | None, PyDoughAST]] | None = None
        self._calc_counter: int = 0

    @property
    def collection(self) -> CollectionMetadata:
        """
        The table that is being referenced by the collection node.
        """
        return self._collection

    @property
    def properties(self) -> Dict[str, Tuple[int | None, PyDoughAST]]:
        """
        Mapping of each property of the table to a tuple (idx, property)
        where idx is the ordinal position of the property when included
        in a CALC (None for subcollections), and property is the AST node
        representing the property.

        The properties are evaluated lazily & cached to prevent ping-ponging
        between two tables that consider each other subcollections.
        """
        from .compound_sub_collection import CompoundSubCollection
        from .sub_collection import SubCollection

        if self._properties is None:
            self._properties = {}
            # Ensure the properties are added in the same order they were
            # defined in the metadata in to ensure dependencies are handled.
            ordered_properties: List[str] = sorted(
                self.collection.get_property_names(),
                key=lambda p: self.collection.definition_order[p],
            )
            for property_name in ordered_properties:
                property: PropertyMetadata = self.collection.get_property(property_name)
                calc_idx: int | None
                expression: PyDoughAST
                if isinstance(property, CompoundRelationshipMetadata):
                    calc_idx = None
                    expression = CompoundSubCollection(self, property)
                elif property.is_subcollection:
                    calc_idx = None
                    expression = SubCollection(self, property)
                else:
                    calc_idx = self._calc_counter
                    expression = ColumnProperty(property)
                    self._calc_counter += 1
                self._properties[property_name] = (calc_idx, expression)
        return self._properties

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return None

    @property
    def preceding_context(self) -> PyDoughCollectionAST | None:
        return None

    @property
    def calc_terms(self) -> Set[str]:
        # The calc terms are just all of the column properties (the ones
        # that have an index)
        return {name for name, (idx, _) in self.properties.items() if idx is not None}

    @property
    def all_terms(self) -> Set[str]:
        return set(self.properties)

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.properties:
            raise PyDoughASTException(
                f"Unrecognized term of {self.collection.error_name}: {expr_name!r}"
            )
        idx, _ = self.properties[expr_name]
        if idx is None:
            raise PyDoughASTException(
                f"Cannot call get_expression_position on non-CALC term: {expr_name!r}"
            )
        return idx

    def get_term(self, term_name: str) -> PyDoughAST:
        if term_name not in self.properties:
            raise PyDoughASTException(
                f"Unrecognized term of {self.collection.error_name}: {term_name!r}"
            )
        _, term = self.properties[term_name]
        return term

    def to_string(self) -> str:
        return self.collection.name

    def to_tree_form(self) -> CollectionTreeForm:
        return CollectionTreeForm(f"TableCollection[{self.collection.name}]", 0)

    def equals(self, other: "TableCollection") -> bool:
        return super().equals(other) and self.collection == other.collection
