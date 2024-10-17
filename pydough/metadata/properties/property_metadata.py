"""
TODO: add file-level docstring
"""

from abc import abstractmethod

from typing import Dict, List
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    PyDoughMetadataException,
    verify_valid_name,
    verify_has_type,
)
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.collections import CollectionMetadata


class PropertyMetadata(AbstractMetadata):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str, collection: CollectionMetadata):
        verify_valid_name(name)
        verify_has_type(collection, CollectionMetadata, "collection")
        self.name = name
        self.collection = collection

    @property
    def error_name(self):
        """
        TODO: add function docstring
        """
        return self.create_error_name(self.name, self.collection.error_name)

    @staticmethod
    @abstractmethod
    def create_error_name(name: str, collection_error_name: str):
        """
        TODO: add function docstring
        """

    @property
    @abstractmethod
    def is_plural(self) -> bool:
        """
        TODO: add function docstring.
        """

    @property
    @abstractmethod
    def is_subcollection(self) -> bool:
        """
        TODO: add function docstring.
        """

    @property
    @abstractmethod
    def is_reversible(self) -> bool:
        """
        TODO: add function docstring.
        """

    @property
    @abstractmethod
    def components(self) -> tuple:
        """
        TODO: add function docstring.
        """
        return self.collection.components + (self.name,)

    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.properties import (
            TableColumnMetadata,
            SimpleJoinMetadata,
            CompoundRelationshipMetadata,
            CartesianProductMetadata,
        )

        verify_valid_name(property_name)
        # Create the string used to identify the property in error messages.
        error_name = f"property {property_name!r} of collection {collection.error_name}"
        verify_json_has_property_with_type(property_json, "type", str, error_name)
        match property_json["type"]:
            case "table_column":
                TableColumnMetadata.verify_json_metadata(
                    collection, property_name, property_json
                )
            case "simple_join":
                SimpleJoinMetadata.verify_json_metadata(
                    collection, property_name, property_json
                )
            case "cartesian":
                CartesianProductMetadata.verify_json_metadata(
                    collection, property_name, property_json
                )
            case "compound":
                CompoundRelationshipMetadata.verify_json_metadata(
                    collection, property_name, property_json
                )
            case property_type:
                raise PyDoughMetadataException(
                    f"Unrecognized property type for {error_name}: {repr(property_type)}"
                )

    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.properties import (
            TableColumnMetadata,
            SimpleJoinMetadata,
            CompoundRelationshipMetadata,
            CartesianProductMetadata,
        )

        PropertyMetadata.verify_json_metadata(collection, property_name, property_json)
        match property_json["type"]:
            case "table_column":
                TableColumnMetadata.parse_from_json(
                    collection, property_name, property_json
                )
            case "simple_join":
                SimpleJoinMetadata.parse_from_json(
                    collection, property_name, property_json
                )
            case "cartesian":
                CartesianProductMetadata.parse_from_json(
                    collection, property_name, property_json
                )
            case "compound":
                CompoundRelationshipMetadata.parse_from_json(
                    collection, property_name, property_json
                )
            case property_type:
                raise Exception(f"Unrecognized property type: {property_type!r}")

    def get_nouns(self) -> Dict[str, List[AbstractMetadata]]:
        return {self.name: [self]}
