"""
TODO: add file-level docstring
"""

from typing import Dict
from .property_metadata import PropertyMetadata
from pydough.metadata.errors import verify_has_type
from .reversible_property_metadata import ReversiblePropertyMetadata
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    verify_json_has_property_matching,
    is_string_string_mapping,
    verify_matches_predicate,
)
from pydough.metadata.collections import CollectionMetadata


class CompoundRelationshipMetadata(ReversiblePropertyMetadata):
    """
    TODO: add class docstring
    """

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
        self.primary_property: PropertyMetadata = primary_property
        self.secondary_property: PropertyMetadata = secondary_property
        self.inherited_properties: Dict[str, PropertyMetadata] = inherited_properties

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
        secondary_property: ReversiblePropertyMetadata = collection.properties[
            primary_property_name
        ]
        other_collection: CollectionMetadata = secondary_property.other_collection

        inherited_properties: Dict[str, ReversiblePropertyMetadata] = {}
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

    def build_reverse_relationship(self) -> None:
        raise NotImplementedError
        # reverse = CompoundRelationshipMetadata(
        #     self.graph_name,
        #     self.reverse_collection.name,
        #     self.reverse_relationship_name,
        # )
        # reverse.singular = self.no_collisions
        # reverse.no_collisions = self.singular
        # reverse.reverse_relationship_name = self.name
        # reverse.inherited_properties_mapping = self.inherited_properties_mapping
        # reverse.inherited_properties = self.inherited_properties

        # reverse.primary_property = self.secondary_property.reverse_property
        # reverse.secondary_property = self.primary_property.reverse_property

        # reverse.primary_property_name = reverse.primary_property.name
        # reverse.secondary_property_name = reverse.secondary_property.name
        # reverse.secondary_collection = self.secondary_collection

        # reverse.collection = self.reverse_collection
        # reverse.reverse_collection = self.collection

        # reverse.reverse_property = self
        # self.reverse_property = reverse
