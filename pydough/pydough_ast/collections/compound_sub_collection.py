"""
TODO: add file-level docstring
"""

__all__ = ["CompoundSubCollection"]


from typing import Dict, List, Tuple, Set

from pydough.metadata import CompoundRelationshipMetadata
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from .collection_ast import PyDoughCollectionAST
from .sub_collection import SubCollection
from .hidden_back_reference_collection import HiddenBackReferenceCollection
from pydough.pydough_ast.expressions.hidden_back_reference_expression import (
    HiddenBackReferenceExpression,
)


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
        root: bool = False,
    ) -> PyDoughCollectionAST:
        """
        TODO: add function docstring
        """
        for idx, property in enumerate(
            [compound.primary_property, compound.secondary_property]
        ):
            if isinstance(property, CompoundRelationshipMetadata):
                for alias, property_name in inherited_properties.items():
                    if property_name in property.inherited_properties:
                        inherited_properties[alias] = property.inherited_properties[
                            property_name
                        ].property_to_inherit.name
                source = self.populate_subcollection_chain(
                    source, property, inherited_properties
                )
            else:
                source_idx: int = len(self._subcollection_chain)
                source = source.get_term(property.name)
                if idx == 0 or not root:
                    found_inherited: Set[str] = set()
                    for alias, property_name in inherited_properties.items():
                        if property_name in property.other_collection.properties:
                            found_inherited.add(alias)
                            self._inheritance_sources[alias] = (
                                source_idx,
                                property_name,
                            )
                            inh_property: PyDoughAST = source.get_term(property_name)
                            if isinstance(inh_property, PyDoughCollectionAST):
                                self._properties[alias] = (None, inh_property)
                            else:
                                self._properties[alias] = (
                                    self._calc_counter,
                                    inh_property,
                                )
                                self._calc_counter += 1
                    for alias in found_inherited:
                        inherited_properties.pop(alias)
                self._subcollection_chain.append(source)

        return source

    @property
    def subcollection_chain(self) -> List[SubCollection]:
        """
        The list of subcollection accesses used to define the compound
        relationship.
        """
        self.properties
        return self._subcollection_chain

    @property
    def inheritance_sources(self) -> Dict[str, Tuple[int, str]]:
        """
        The mapping between each inherited property name and the integer
        position of the subcollection access it corresponds to from within
        the subcollection chain, as well as the name it had within that
        regular collection.
        """
        self.properties
        return self._inheritance_sources

    @property
    def properties(self) -> Dict[str, Tuple[int | None, PyDoughAST]]:
        if self._properties is None:
            self._properties = super().properties
            compound: CompoundRelationshipMetadata = self.subcollection_property
            inherited_map: Dict[str, str] = {
                name: property.property_to_inherit.name
                for name, property in compound.inherited_properties.items()
            }
            self.populate_subcollection_chain(
                self.parent, self.subcollection_property, inherited_map, root=True
            )
            undefined_inherited: Set[str] = set(compound.inherited_properties) - set(
                self.inheritance_sources
            )
            if len(undefined_inherited) > 0:
                raise PyDoughASTException(
                    f"Undefined inherited properties: {undefined_inherited}"
                )
            for alias, (location, original_name) in self._inheritance_sources.items():
                calc_idx, expr = self._properties[alias]
                ancestor: PyDoughCollectionAST = self._subcollection_chain[location]
                back_levels: int = len(self.subcollection_chain) - location
                if isinstance(expr, PyDoughCollectionAST):
                    self._properties[alias] = (
                        calc_idx,
                        HiddenBackReferenceCollection(
                            self, ancestor, original_name, back_levels
                        ),
                    )
                else:
                    self._properties[alias] = (
                        calc_idx,
                        HiddenBackReferenceExpression(
                            self, ancestor, original_name, back_levels
                        ),
                    )
        return self._properties

    def to_tree_string(self) -> str:
        raise NotImplementedError
