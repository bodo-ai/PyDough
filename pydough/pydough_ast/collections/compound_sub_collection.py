"""
TODO: add file-level docstring
"""

__all__ = ["CompoundSubCollection"]


from typing import Dict, List, Tuple

from pydough.metadata import CompoundRelationshipMetadata
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from .collection_ast import PyDoughCollectionAST
from .sub_collection import SubCollection


class CompoundSubCollection(SubCollection):
    """
    The AST node implementation class representing a subcollection accessed
    from its parent collection which is via a compound relationship.
    """

    def __init__(
        self,
        parent: PyDoughCollectionAST,
        subcollection_property: CompoundRelationshipMetadata,
    ):
        super().__init__(parent, subcollection_property)
        self._subcollection_chain: List[SubCollection] = []
        self._inheritance_sources: Dict[str, Tuple[int, str]] = {}

    def populate_subcollection_chain(
        self,
        source: PyDoughCollectionAST,
        compound: CompoundRelationshipMetadata,
        inherited_properties: Dict[str, str],
    ) -> PyDoughCollectionAST:
        """ """
        for property in [compound.primary_property, compound.secondary_property]:
            # new_inherited_properties: Dict[str, str] = {}
            if isinstance(property, CompoundRelationshipMetadata):
                source = self.populate_subcollection_chain(source, property)
            else:
                # source_idx: int = len(self._inheritance_sources)
                source = source.get_term(property.name)
                self._subcollection_chain.append(source)
                # for alias, property_name in inherited_properties:
                #     pass
            # inherited_properties = new_inherited_properties

        return source

    @property
    def subcollection_chain(self) -> List[SubCollection]:
        """
        The list of subcollection accesses used to define the compound
        relationship.
        """
        return self._subcollection_chain

    @property
    def inheritance_sources(self) -> Dict[str, Tuple[int, str]]:
        """
        The mapping between each inherited property name and the integer
        position of the subcollection access it corresponds to from within
        the subcollection chain, as well as the name it had within that
        regular collection.
        """
        return self._inheritance_sources

    @property
    def properties(self) -> Dict[str, Tuple[int | None, PyDoughAST]]:
        if self._properties is None:
            self._properties = super().properties
            compound: CompoundRelationshipMetadata = self.subcollection_property
            inherited_map: Dict[str, str] = {
                name: property.name
                for name, property in compound.inherited_properties.items()
            }
            self.populate_subcollection_chain(
                self.parent, self.subcollection_property, inherited_map
            )
            breakpoint()
            # if isinstance(self.subcollection_property, CompoundRelationshipMetadata):

            #     primary: SubcollectionRelationshipMetadata = self.subcollection_property.primary_property
            #     middle: CollectionMetadata = primary.other_collection
            #     secondary: SubcollectionRelationshipMetadata = self.subcollection_property.secondary_property
            #     middle.inherited_properties
            #     for property_name, property in sorted(self.subcollection_property.inherited_properties.items()):
            #         inherited_property: InheritedPropertyMetadata = property
            #         inherited_property.property_inherited_from
            #         inherited_property.property_to_inherit
            #         calc_idx: int | None
            #         expression: PyDoughAST
            #         # breakpoint()
            #         if property.is_subcollection:
            #             calc_idx = None
            #             expression = SubCollection(self, property.property_to_inherit)
            #         else:
            #             calc_idx = self._calc_counter
            #             expression = ColumnProperty(property)
            #             self._calc_counter += 1
            #         self._properties[property_name] = (calc_idx, expression)
        return self._properties

    def to_string(self) -> str:
        component_strings: List[str] = []
        for subcollection in self.subcollection_chain:
            component_strings.append(subcollection.subcollection_property.name)
        return f"{self.parent.to_string()}.({self.subcollection_property.name}={'.'.join(component_strings)})"

    def to_tree_string(self) -> str:
        raise NotImplementedError
