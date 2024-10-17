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
    TODO: add class docstring
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
        self.primary_property: ReversiblePropertyMetadata = primary_property
        self.secondary_property: ReversiblePropertyMetadata = secondary_property
        self.inherited_properties: Dict[str, PropertyMetadata] = {}
        for alias, property in inherited_properties.items():
            self.inherited_properties[alias] = InheritedPropertyMetadata(
                alias, other_collection, self, property
            )

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
        # Create the string used to identify the property in error messages.
        error_name = f"compound relationship property {property_name!r} of {CollectionMetadata.error_name}"
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
        primary_property_name: str = property_json["primary_property"]
        secondary_property_name: str = property_json["secondary_property"]
        inherited_properties_mapping: Dict[str, str] = property_json[
            "inherited_properties"
        ]
        singular: bool = property_json["singular"]
        no_collisions: bool = property_json["no_collisions"]
        reverse_name: str = property_json["reverse_relationship_name"]

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

        for inherited_property in property.inherited_properties.values():
            other_collection.add_inherited_property(inherited_property)
        for (
            inherited_property
        ) in property.reverse_property.inherited_properties.values():
            other_collection.add_inherited_property(inherited_property)

    def build_reverse_relationship(self) -> None:
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
        reverse.reverse_property = self
        self.reverse_property = reverse
