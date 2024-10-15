"""
TODO: add file-level docstring
"""

from abc import abstractmethod

from typing import List, Dict
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    PyDoughMetadataException,
    verify_valid_name,
    verify_has_type,
)

from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.graphs import GraphMetadata


class CollectionMetadata(AbstractMetadata):
    """
    TODO: add class docstring
    """

    allowed_properties = ["type", "properties"]

    def __init__(self, name: str, graph: GraphMetadata):
        from pydough.metadata.properties import (
            PropertyMetadata,
            InheritedPropertyMetadata,
        )

        verify_valid_name(name)
        verify_has_type(graph, GraphMetadata, "Graph of CollectionMetadata")
        self.graph: GraphMetadata = graph
        self.name: str = name
        self.properties: Dict[str, PropertyMetadata] = {}
        self.inherited_properties: Dict[str, InheritedPropertyMetadata] = {}

    @property
    def error_name(self):
        """
        TODO: add function docstring
        """
        return self.create_error_name(self.name, self.graph.error_name)

    @abstractmethod
    def create_error_name(name: str, graph_error_name: str):
        """
        TODO: add function docstring
        """

    @property
    @abstractmethod
    def components(self) -> tuple:
        """
        TODO: add function docstring.
        """
        return self.graph.components + (self.graph.name, self.name)

    @abstractmethod
    def verify_allows_property(
        self, property: AbstractMetadata, inherited: bool
    ) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.properties import (
            PropertyMetadata,
            InheritedPropertyMetadata,
        )

        verify_has_type(property, PropertyMetadata, "property")
        property: PropertyMetadata = property
        if inherited:
            if not isinstance(property, InheritedPropertyMetadata):
                raise PyDoughMetadataException(
                    f"Property argument to add_inherited_property must be an InheritedPropertyMetadata. Received a {property.__class__.__name__}"
                )
        else:
            if isinstance(property, InheritedPropertyMetadata):
                raise PyDoughMetadataException(
                    "Cannot add an inherited property with add_property. Use add_inherited_property instead."
                )

        if property.name in self.properties:
            if self.properties[property.name] == property:
                raise PyDoughMetadataException(
                    f"Already added {property.error_name} to {self.error_name}"
                )
            raise PyDoughMetadataException(
                f"Duplicate property name {property.error_name!r} in {self.error_name}: {property.error_name} versus {self.properties[property.name].error_name}."
            )

        if property.name in self.inherited_properties:
            if self.inherited_properties[property.name] == property:
                raise PyDoughMetadataException(
                    f"Already added {property.error_name} to {self.error_name}"
                )
            raise PyDoughMetadataException(
                f"Inherited property {self.properties[property.name].error_name} is a duplicate of an existing property {property.error_name}."
            )

    def add_property(
        self, property: AbstractMetadata, adding_reverse: bool = False
    ) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.properties import PropertyMetadata

        property: PropertyMetadata = property
        self.verify_allows_property(property, False)
        self.properties[property.name] = property
        if not adding_reverse and property.is_reversible:
            reverse_property = property.reverse_property
            property.reverse_collection.add_property(reverse_property, True)

    def add_inherited_property(self, property: AbstractMetadata) -> None:
        from pydough.metadata.properties import PropertyMetadata

        property: PropertyMetadata = property
        self.verify_allows_property(property, False)
        self.inherited_properties[property.name] = property

    def get_nouns(self) -> List[AbstractMetadata]:
        nouns = [(self.name, self)]
        for property in self.properties.values():
            nouns.append((property.name, property))
        for property in self.inherited_properties.values():
            nouns.append((property.name, property))
        return nouns

    def get_property_names(self) -> List[str]:
        """
        TODO: add function docstring.
        """
        return list(self.properties)

    def get_property(self, property_name: str) -> AbstractMetadata:
        """
        TODO: add function docstring.
        """
        if property_name not in self.properties:
            raise PyDoughMetadataException(
                f"Collection {self.name} does not have a property {property_name!r}"
            )
        return self.properties[property_name]

    @abstractmethod
    def verify_json_metadata(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        verify_valid_name(collection_name)
        verify_has_type(graph, GraphMetadata, "graph")
        error_name = f"collection {collection_name!r} in {graph.error_name}"
        if collection_name == graph.name:
            raise PyDoughMetadataException(
                f"Cannot have collection named {collection_name!r} share the same name as the graph containing it."
            )

        verify_json_has_property_with_type(collection_json, "type", str, error_name)
        verify_json_has_property_with_type(
            collection_json, "properties", dict, error_name
        )

    @abstractmethod
    def parse_from_json(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.collections import SimpleTableMetadata

        CollectionMetadata.verify_json_metadata(graph, collection_name, collection_json)

        collection: CollectionMetadata
        match collection_json["type"]:
            case "simple_table":
                collection = SimpleTableMetadata.parse_from_json(
                    graph, collection_name, collection_json
                )
            case collection_type:
                raise Exception(f"Unrecognized collection type: '{collection_type}'")

        graph.add_collection(collection)
