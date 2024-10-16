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
import itertools
from collections import defaultdict
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
        return CollectionMetadata.create_error_name(self.name, self.graph.error_name)

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
        return self.graph.components + (self.name,)

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

    def add_property(self, property: AbstractMetadata) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.properties import PropertyMetadata

        property: PropertyMetadata = property
        self.verify_allows_property(property, False)
        self.properties[property.name] = property

    def add_inherited_property(self, property: AbstractMetadata) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.properties import PropertyMetadata

        property: PropertyMetadata = property
        self.verify_allows_property(property, False)
        self.inherited_properties[property.name] = property

    def get_nouns(self) -> Dict[str, List[AbstractMetadata]]:
        nouns: Dict[str, List[AbstractMetadata]] = defaultdict(list)
        for property in itertools.chain(
            self.properties.values(), self.inherited_properties.values()
        ):
            for noun_name, values in property.get_nouns():
                nouns[noun_name].extend(values)
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
        from . import SimpleTableMetadata

        CollectionMetadata.verify_json_metadata(graph, collection_name, collection_json)

        match collection_json["type"]:
            case "simple_table":
                SimpleTableMetadata.parse_from_json(
                    graph, collection_name, collection_json
                )
            case collection_type:
                raise Exception(f"Unrecognized collection type: '{collection_type}'")
