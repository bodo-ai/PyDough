"""
TODO: add file-level docstring
"""

from abc import ABC, abstractmethod

from typing import List, Union, Dict, Tuple
from pydough.metadata.errors import (
    PyDoughMetadataException,
    verify_valid_name,
    verify_typ_in_json,
)
from pydough.metadata.properties import PropertyMetadata, InheritedPropertyMetadata


class CollectionMetadata(ABC):
    """
    TODO: add class docstring
    """

    allowed_properties = ["type", "properties"]

    def __init__(self, graph_name: str, name: str):
        self.graph_name: str = graph_name
        self.name: str = name
        self.properties: Dict[str, PropertyMetadata] = {}
        self.inherited_properties: Dict[str, Tuple[InheritedPropertyMetadata]] = {}

    @abstractmethod
    def components(self) -> Tuple:
        """
        TODO: add function doscstring.
        """
        return (self.graph_name, self.name)

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(repr(component) for component in self.components())})"

    def __eq__(self, other):
        return (type(self) is type(other)) and (self.components() == other.components())

    def verify_json_metadata(
        graph_name: str, collection_name: str, graph_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        from pydough.metadata.collections import SimpleTableMetadata

        verify_valid_name(collection_name)
        collection_json = graph_json[collection_name]
        error_name = f"collection {repr(collection_name)} in graph {repr(graph_name)}"
        if collection_name == graph_name:
            raise PyDoughMetadataException(
                f"Cannot have collection named {repr(collection_name)} share the same name as the graph containing it."
            )

        verify_typ_in_json(collection_json, "type", str, error_name)
        verify_typ_in_json(collection_json, "properties", dict, error_name)

        match collection_json["type"]:
            case "simple_table":
                SimpleTableMetadata.verify_json_metadata(
                    graph_name, collection_name, collection_json
                )
            case collection_type:
                raise PyDoughMetadataException(
                    f"Unrecognized collection type for {error_name}: {repr(collection_type)}"
                )

        properties_json = collection_json["properties"]
        for property_name in collection_json["properties"]:
            verify_typ_in_json(properties_json, property_name, dict, error_name)
            PropertyMetadata.verify_json_metadata(
                graph_name,
                collection_name,
                property_name,
                properties_json[property_name],
            )

    @abstractmethod
    def parse_from_json(self, graph_json: dict) -> None:
        """
        TODO: add function doscstring.
        """

    @abstractmethod
    def verify_is_property_valid_for_collection(
        self, property: PropertyMetadata
    ) -> None:
        """
        TODO: add function doscstring.
        """

    def add_property(self, property: PropertyMetadata) -> None:
        """
        TODO: add function doscstring.
        """
        if not isinstance(property, PropertyMetadata):
            raise PyDoughMetadataException(
                f"Property argument to add_property must be a PropertyMetadata. Received a {property.__class__.__name__}"
            )
        if isinstance(property, InheritedPropertyMetadata):
            raise PyDoughMetadataException(
                "Cannot add an inherited property with add_property. Use add_inherited_property instead."
            )

        self.verify_is_property_valid_for_collection(property)
        error_name = f"collection {repr(self.name)} of graph {self.graph_name}"

        if property.name in self.properties:
            raise PyDoughMetadataException(
                f"Duplicate property name {repr(property)} in {error_name}."
            )
        if property.name in self.inherited_properties:
            inherited_property: InheritedPropertyMetadata = self.inherited_properties[
                property.name
            ]
            ancestry = f"{inherited_property.original_collection_name}.{inherited_property.primary_subcollection_collection_name}.{inherited_property.secondary_subcollection_name}"
            raise PyDoughMetadataException(
                f"Inherited property {repr(property)} (from {ancestry}) in {error_name} is a duplicate property name of an existing property."
            )

        self.properties[property.name] = property

    def add_inherited_property(self, property: InheritedPropertyMetadata) -> None:
        if not isinstance(property, PropertyMetadata):
            raise PyDoughMetadataException(
                f"Property argument to add_inherited_property must be an InheritedPropertyMetadata. Received a {property.__class__.__name__}"
            )

        self.verify_is_property_valid_for_collection(property)
        error_name = f"collection {repr(self.name)} of graph {self.graph_name}"
        ancestry = f"{property.original_collection_name}.{property.primary_subcollection_collection_name}.{property.secondary_subcollection_name}"
        if property.name in self.properties:
            raise PyDoughMetadataException(
                f"Inherited property {repr(property)} (from {ancestry}) in {error_name} is a duplicate property name of an existing property."
            )
        if property.name in self.inherited_properties:
            inherited_property: InheritedPropertyMetadata = self.inherited_properties[
                property.name
            ]
            secondary_ancestry = f"{inherited_property.original_collection_name}.{inherited_property.primary_subcollection_collection_name}.{inherited_property.secondary_subcollection_name}"
            raise PyDoughMetadataException(
                f"Inherited property {repr(property)} (from {ancestry}) in {error_name} is a duplicate property name of another inherited property (from {secondary_ancestry})."
            )

        self.inherited_properties[property.name] = property

    def get_nouns(
        self,
    ) -> List[Tuple[str, Union["CollectionMetadata", PropertyMetadata]]]:
        """
        TODO: add function doscstring.
        """
        nouns = [(self.name, self)]
        for property in self.properties.values():
            nouns.append((property.name, property))
        for property in self.inherited_properties.values():
            nouns.append((property.name, property))
        return nouns

    def get_property_names(self) -> List[str]:
        """
        TODO: add function doscstring.
        """
        return list(self.properties)

    def get_property(self, property_name: str) -> PropertyMetadata:
        """
        TODO: add function doscstring.
        """
        if property_name not in self.properties:
            raise PyDoughMetadataException(
                f"Collection {self.name} does not have a property {repr(property_name)}"
            )
        return self.properties[property_name]
