"""
TODO: add file-level docstring
"""

from typing import Dict, List
from pydough.metadata.errors import verify_has_type
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    verify_json_has_property_matching,
    is_string_string_mapping,
    verify_matches_predicate,
    verify_no_extra_keys_in_json,
)
from pydough.metadata.collections import CollectionMetadata
from . import PropertyMetadata, ReversiblePropertyMetadata


class CompoundRelationshipMetadata(ReversiblePropertyMetadata):
    """
    Concrete metadata implementation for a PyDough property created by
    combining two reversible properties, one mapping a collection to
    one of its subcollections and the other mapping that subcollection
    to one of its subcollections. A property also grants access to
    certain inherited properties derived from the middle collection.
    """

    # List of names of of fields that can be included in the JSON object
    # describing a compound relationship property.
    allowed_fields: List[str] = PropertyMetadata.allowed_fields + [
        "primary_property",
        "secondary_property",
        "reverse_relationship_name",
        "singular",
        "no_collisions",
        "inherited_properties",
    ]

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
        singular: bool,
        no_collisions: bool,
        primary_property: ReversiblePropertyMetadata,
        secondary_property: ReversiblePropertyMetadata,
        inherited_properties: Dict[str, PropertyMetadata],
    ):
        from .inherited_property_metadata import InheritedPropertyMetadata

        super().__init__(
            name, reverse_name, collection, other_collection, singular, no_collisions
        )
        verify_has_type(primary_property, ReversiblePropertyMetadata, self.error_name)
        verify_has_type(secondary_property, ReversiblePropertyMetadata, self.error_name)
        verify_matches_predicate(
            inherited_properties,
            lambda x: isinstance(x, dict)
            and all(
                isinstance(k, str) and isinstance(v, PropertyMetadata)
                for k, v in x.items()
            ),
            "inherited_properties",
            "mapping of valid Python identifiers to reversible properties",
        )
        self._primary_property: ReversiblePropertyMetadata = primary_property
        self._secondary_property: ReversiblePropertyMetadata = secondary_property
        self._inherited_properties: Dict[str, PropertyMetadata] = {}
        for alias, property in inherited_properties.items():
            self.inherited_properties[alias] = InheritedPropertyMetadata(
                alias, other_collection, self, property
            )

    @property
    def primary_property(self) -> ReversiblePropertyMetadata:
        """
        The property used to map the collection to the middle collection.
        """
        return self._primary_property

    @property
    def secondary_property(self) -> ReversiblePropertyMetadata:
        """
        The property used to map the middle collection to the other collection.
        """
        return self._secondary_property

    @property
    def inherited_properties(self) -> Dict[str, PropertyMetadata]:
        """
        The properties inherited by using this compound relationship,
        represented as a mapping of an alias name to the actual property.
        """
        return self._inherited_properties

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"compound property {name!r} of {collection_error_name}"

    @property
    def components(self) -> tuple:
        inherited_properties_dict = {
            alias: property.name
            for alias, property in self.inherited_properties.items()
        }
        return super().components + (
            self.primary_property.name,
            self.secondary_property.name,
            inherited_properties_dict,
        )

    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Verifies that the JSON describing the metadata for a property within
        a collection is well-formed to create a new
        CompoundRelationshipMetadata instance. Should be dispatched from
        PropertyMetadata.verify_json_metadata which implements more generic
        checks.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted into.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Create the string used to identify the property in error messages.
        error_name = f"compound relationship property {property_name!r} of {CollectionMetadata.error_name}"

        # Verify that the JSON has the required `primary_property`,
        # `secondary_property`, `reverse_relationship_name`, `singular`,
        # `no_collisions` and `inherited_properties` fields, without anything
        # extra.
        verify_json_has_property_with_type(
            property_json, "primary_property", str, error_name
        )
        verify_json_has_property_with_type(
            property_json, "secondary_property", str, error_name
        )
        verify_json_has_property_with_type(
            property_json, "reverse_relationship_name", str, error_name
        )
        verify_json_has_property_with_type(property_json, "singular", bool, error_name)
        verify_json_has_property_with_type(
            property_json, "no_collisions", bool, error_name
        )
        verify_json_has_property_matching(
            property_json,
            "inherited_properties",
            lambda x: is_string_string_mapping(x, True),
            error_name,
            "JSON object of strings",
        )
        verify_no_extra_keys_in_json(
            property_json, CompoundRelationshipMetadata.allowed_fields, error_name
        )

    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure dispatched from PropertyMetadata.parse_from_json to handle
        the parsing for compound relationship properties.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted nto.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new table column property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Extract the name of the primary/secondary properties, the inherited
        # properties mapping, the reverse relationship name, and the singular /
        # no_collisions fields from the JSON.
        primary_property_name: str = property_json["primary_property"]
        secondary_property_name: str = property_json["secondary_property"]
        inherited_properties_mapping: Dict[str, str] = property_json[
            "inherited_properties"
        ]
        singular: bool = property_json["singular"]
        no_collisions: bool = property_json["no_collisions"]
        reverse_name: str = property_json["reverse_relationship_name"]

        # Extract the primary property from the current collection's
        # properties. Assumes that the primary property has already been
        # defined and added to the collection's properties.
        verify_json_has_property_with_type(
            collection.properties,
            primary_property_name,
            ReversiblePropertyMetadata,
            collection.error_name,
        )
        primary_property: ReversiblePropertyMetadata = collection.properties[
            primary_property_name
        ]
        secondary_collection: CollectionMetadata = primary_property.other_collection

        # Extract the secondary property from the middle collection's
        # properties. Assumes that the secondary property has already been
        # defined and added to the middle collection's properties.
        verify_json_has_property_with_type(
            secondary_collection.properties,
            secondary_property_name,
            ReversiblePropertyMetadata,
            secondary_collection.error_name,
        )
        secondary_property: ReversiblePropertyMetadata = (
            secondary_collection.properties[secondary_property_name]
        )
        other_collection: CollectionMetadata = secondary_property.other_collection

        # Obtain the inherited properties by mapping each of the alias names in
        # the JSON to the corresponding property metadata from the desired
        # property of the middle collection. Assumes that all the inherited
        # properties have already been defined added to the middle collection's
        # properties.
        inherited_properties: Dict[str, PropertyMetadata] = {}
        for alias_name, inherited_property_name in inherited_properties_mapping.items():
            verify_json_has_property_with_type(
                secondary_collection.properties,
                inherited_property_name,
                PropertyMetadata,
                secondary_collection.error_name,
            )
            inherited_property: PropertyMetadata = secondary_collection.properties[
                inherited_property_name
            ]
            inherited_properties[alias_name] = inherited_property

        # Build the new property, its reverse, then add both
        # to their collection's properties.
        property = CompoundRelationshipMetadata(
            property_name,
            reverse_name,
            collection,
            other_collection,
            singular,
            no_collisions,
            primary_property,
            secondary_property,
            inherited_properties,
        )
        property.build_reverse_relationship()
        collection.add_property(property)
        other_collection.add_property(property.reverse_property)

        # Add the inherited properties from the compound relationship
        # and its reverse to the collection & subcollection it maps to.
        for inherited_property in property.inherited_properties.values():
            other_collection.add_inherited_property(inherited_property)
        for (
            inherited_property
        ) in property.reverse_property.inherited_properties.values():
            other_collection.add_inherited_property(inherited_property)

    def build_reverse_relationship(self) -> None:
        # Construct the reverse relationship by flipping the forward & reverse
        # names, the source / target collections, and the plural properties.
        # The new primary property is the reverse of the secondary property.
        # The new secondary property is the reverse of the primary property.
        reverse = CompoundRelationshipMetadata(
            self.reverse_name,
            self.name,
            self.other_collection,
            self.collection,
            self.no_collisions,
            self.singular,
            self.secondary_property.reverse_property,
            self.primary_property.reverse_property,
            self.inherited_properties,
        )

        # Then fill the `reverse_property` fields with one another.
        reverse._reverse_property = self
        self._reverse_property = reverse
