"""
Definition of PyDough QDAG collection type for accesses to a subcollection of the
current context where the subcollection is a compound relationship.
"""

__all__ = ["CompoundSubCollection"]


from functools import cache

from pydough.metadata import CompoundRelationshipMetadata
from pydough.metadata.properties import InheritedPropertyMetadata
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.qdag.expressions.hidden_back_reference_expression import (
    HiddenBackReferenceExpression,
)

from .collection_access import CollectionAccess
from .collection_qdag import PyDoughCollectionQDAG
from .sub_collection import SubCollection


class CompoundSubCollection(SubCollection):
    """
    The QDAG node implementation class representing a subcollection accessed
    from its parent collection which is via a compound relationship.
    """

    def __init__(
        self,
        compound: CompoundRelationshipMetadata,
        ancestor: PyDoughCollectionQDAG,
    ):
        super().__init__(compound, ancestor)
        self._subcollection_chain: list[SubCollection] = []
        self._inheritance_source_idx: dict[str, int] = {}
        self._inheritance_source_name: dict[str, str] = {}

        assert isinstance(compound, CompoundRelationshipMetadata)
        inherited_map: dict[str, str] = {}
        for name, property in compound.inherited_properties.items():
            assert isinstance(property, InheritedPropertyMetadata)
            inherited_map[name] = property.property_to_inherit.name
            self._all_property_names.add(name)
            if not property.is_subcollection:
                self._calc_property_names.add(name)
                self._calc_property_order[name] = len(self._calc_property_order)
        ancestor_context = self.ancestor_context
        assert ancestor_context is not None
        self.populate_subcollection_chain(ancestor_context, compound, inherited_map)
        undefined_inherited: set[str] = set(compound.inherited_properties) - set(
            self._inheritance_source_name
        )
        if len(undefined_inherited) > 0:
            raise PyDoughQDAGException(
                f"Undefined inherited properties: {undefined_inherited}"
            )

    def clone_with_parent(
        self, new_ancestor: PyDoughCollectionQDAG
    ) -> CollectionAccess:
        assert isinstance(self.subcollection_property, CompoundRelationshipMetadata)
        return CompoundSubCollection(self.subcollection_property, new_ancestor)

    def populate_subcollection_chain(
        self,
        source: PyDoughCollectionQDAG,
        compound: CompoundRelationshipMetadata,
        inherited_properties: dict[str, str],
    ) -> SubCollection:
        """
        Recursive procedure used to define the `subcollection_chain` and
        `inheritance_sources` fields of a compound subcollection QDAG node. In
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
            The subcollection QDAG object corresponding to the last component
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
                new_inherited_properties: dict[str, str] = {}
                for alias, property_name in inherited_properties.items():
                    if property_name in property.inherited_properties:
                        inh = property.inherited_properties[property_name]
                        assert isinstance(inh, InheritedPropertyMetadata)
                        new_inherited_properties[alias] = inh.property_to_inherit.name
                    if property_name in property.other_collection.properties:
                        new_inherited_properties[alias] = property_name
                for alias in new_inherited_properties:
                    inherited_properties.pop(alias)
                source = self.populate_subcollection_chain(
                    source, property, new_inherited_properties
                )
            else:
                # Otherwise, we are in a base case where we have found a true
                # subcollection invocation. We maintain a set to mark which
                # inherited properties were found.
                term = source.get_collection(property.name)
                assert isinstance(term, SubCollection)
                source = term
                found_inherited: set[str] = set()
                # Iterate through all the remaining inherited properties to
                # find any whose true name matches one of the properties of
                # the target collection. If so, they belong to the
                # subcollection at this stage of the chain.
                for alias, property_name in inherited_properties.items():
                    if property_name in property.other_collection.properties:
                        found_inherited.add(alias)
                        self._inheritance_source_idx[alias] = len(
                            self._subcollection_chain
                        )
                        self._inheritance_source_name[alias] = property_name
                # Remove any inherited properties found from the dictionary so
                # subsequent recursive calls do not try to find the same
                # inherited property (e.g. if the final collection mapped to
                # has a property with the same name as an inherited property's
                # true property name).
                for alias in found_inherited:
                    inherited_properties.pop(alias)
                # Finally, add the new subcollection to the end of the chain.
                self._subcollection_chain.append(source)

        assert isinstance(source, SubCollection)
        return source

    @property
    def subcollection_chain(self) -> list[SubCollection]:
        """
        The list of subcollection accesses used to define the compound
        relationship.
        """
        return self._subcollection_chain

    @property
    def inheritance_source_idx(self) -> dict[str, int]:
        """
        The mapping between each inherited property name and the integer
        position of the subcollection access it corresponds to from within
        the subcollection chain, as well as the name it had within that
        regular collection.
        """
        return self._inheritance_source_idx

    @property
    def inheritance_source_name(self) -> dict[str, int]:
        """
        The mapping between each inherited property name and the name it had in
        the subcollection access it corresponds to from within the subcollection
        chain.
        """
        return self._inheritance_source_idx

    @cache
    def get_term(self, term_name: str) -> PyDoughQDAG:
        assert isinstance(self.subcollection_property, CompoundRelationshipMetadata)
        if term_name in self.subcollection_property.inherited_properties:
            source_idx: int = self.inheritance_source_idx[term_name]
            back_levels: int = len(self.subcollection_chain) - source_idx
            ancestor: PyDoughCollectionQDAG = self._subcollection_chain[source_idx]
            original_name: str = self._inheritance_source_name[term_name]
            expr = ancestor.get_term(original_name)
            if isinstance(expr, PyDoughCollectionQDAG):
                raise NotImplementedError(
                    f"Cannot access subcollection property {term_name} of compound subcollection {self.subcollection_property.name}"
                )
            else:
                return HiddenBackReferenceExpression(
                    self, ancestor, term_name, original_name, back_levels
                )
        else:
            return super().get_term(term_name)
