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
        subcollection_property: CompoundRelationshipMetadata,
        ancestor: PyDoughCollectionAST,
    ):
        super().__init__(subcollection_property, ancestor)
        self._subcollection_chain: List[SubCollection] = []
        self._inheritance_sources: Dict[str, Tuple[int, str]] = {}

    def populate_subcollection_chain(
        self,
        source: PyDoughCollectionAST,
        compound: CompoundRelationshipMetadata,
        inherited_properties: Dict[str, str],
    ) -> PyDoughCollectionAST:
        """
        Recursive procedure used to define the `subcollection_chain` and
        `inheritance_sources` fields of a compound subcollection AST node. In
        the end, results in the compound relationship being fully flattened
        into a sequence of regular subcollection accesses.

        Args:
            `source`: the most recent collection before the next subcollection
            to be defined.
            `compound`: the compound relationship that is currently being
            broken up into 2+ pieces to append to the subcollection chain.
            `inherited_properties`: a mapping of inherited property names (from
            the original compound property) to the name they currently are
            assumed to have within the context of the compound property's
            components.

        Returns:
            The subcollection AST object corresponding to the last component
            of `compound`, once flattened.
        """
        # Invoke the procedure for the primary and secondary property.
        for property in [compound.primary_property, compound.secondary_property]:
            if isinstance(property, CompoundRelationshipMetadata):
                # If the component property is also a compound, recursively repeat
                # the procedure on it, updating the `source` as we go along. First,
                # update the inherited properties dictionary to change the true
                # names of the inherited properties to be whatever their true names
                # are inside the nested compound.
                for alias, property_name in inherited_properties.items():
                    if property_name in property.inherited_properties:
                        inherited_properties[alias] = property.inherited_properties[
                            property_name
                        ].property_to_inherit.name
                source = self.populate_subcollection_chain(
                    source, property, inherited_properties
                )
            else:
                # Otherwise, we are in a base case where we have found a true
                # subcollection invocation. We maintain a set to mark which
                # inherited properties were found.
                source = source.get_term(property.name)
                found_inherited: Set[str] = set()
                # Iterate through all the remaining inherited properties to
                # find any whose true name matches one of the properties of
                # the target collection. If so, they belong to the
                # subcollection at this stage of the chain.
                for alias, property_name in inherited_properties.items():
                    if property_name in property.other_collection.properties:
                        found_inherited.add(alias)
                        self._inheritance_sources[alias] = (
                            len(self._subcollection_chain),
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
                # Remove any inherited properties found from the dictionary so
                # subsequent recursive calls do not try to find the same
                # inherited property (e.g. if the final collection mapped to
                # has a property with the same name as an inherited property's
                # true property name).
                for alias in found_inherited:
                    inherited_properties.pop(alias)
                # Finally, add the new subcollection to the end of the chain.
                self._subcollection_chain.append(source)

        return source

    @property
    def subcollection_chain(self) -> List[SubCollection]:
        """
        The list of subcollection accesses used to define the compound
        relationship.
        """
        # Ensure the lazy evaluation of `self.properties` has been completed
        # so we know that the subcollection chain has been populated.
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
        # Ensure the lazy evaluation of `self.properties` has been completed
        # so we know that the inheritance sources have been populated.
        self.properties
        return self._inheritance_sources

    @property
    def properties(self) -> Dict[str, Tuple[int | None, PyDoughAST]]:
        # Lazily define the properties, if not already defined.
        if self._properties is None:
            # First invoke the TableCollection version to get the regular
            # properties of the target collection added.
            self._properties = super().properties
            # Then, use the recursive `populate_subcollection_chain` to flatten
            # the subcollections used in the compound into a sequence of
            # regular subcollection AST nodes and identify where each inherited
            # term came from.
            compound: CompoundRelationshipMetadata = self.subcollection_property
            inherited_map: Dict[str, str] = {
                name: property.property_to_inherit.name
                for name, property in compound.inherited_properties.items()
            }
            self.populate_subcollection_chain(
                self.ancestor_context, self.subcollection_property, inherited_map
            )
            # Make sure none of the inherited terms went unaccounted for.
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
                            self, ancestor, alias, original_name, back_levels
                        ),
                    )
                else:
                    self._properties[alias] = (
                        calc_idx,
                        HiddenBackReferenceExpression(
                            self, ancestor, alias, original_name, back_levels
                        ),
                    )
        return self._properties
