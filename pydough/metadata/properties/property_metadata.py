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
    Abstract base class for PyDough metadata for properties.

    Each implementation must include the following APIs:
    - `create_error_name`
    - `components`
    - `is_plural`
    - `is_subcollection`
    - `is_reversible`
    """

    # List of names of of fields that can be included in the JSON object
    # describing a property. Implementations should extend this.
    allowed_fields: List[str] = ["type"]

    def __init__(self, name: str, collection: CollectionMetadata):
        verify_valid_name(name)
        verify_has_type(collection, CollectionMetadata, "collection")
        self._name: str = name
        self._collection: CollectionMetadata = collection

    @property
    def name(self) -> str:
        return self._name

    @property
    def collection(self) -> CollectionMetadata:
        return self._collection

    @property
    def error_name(self) -> str:
        return self.create_error_name(self.name, self.collection.error_name)

    @staticmethod
    @abstractmethod
    def create_error_name(name: str, collection_error_name: str):
        """
        Creates a string used for the purposes of the `error_name` property.

        Args:
            `name`: the name of the property.
            `collection_error_name`: the error_name property of the collection
            containing the property.

        Returns:
            The string to use to identify the property in exception messages.
        """

    @property
    @abstractmethod
    def is_plural(self) -> bool:
        """
        True if the property can map each record of the current collection to
        multiple values. False if the property can only map each record of the
        current collection to at most one value.
        """

    @property
    @abstractmethod
    def is_subcollection(self) -> bool:
        """
        True if the property maps the collection to another collection. False
        if it maps it to an expression.
        """

    @property
    @abstractmethod
    def is_reversible(self) -> bool:
        """
        True if the property has a corresponding reverse relationship mapping
        entries in subcollection back to entries in the current collection.
        """

    @property
    @abstractmethod
    def components(self) -> tuple:
        return self.collection.components + (self.name,)

    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Verifies that the JSON describing the metadata for a property within
        a collection is well-formed before parsing it to create the property
        and insert into the collection.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted nto.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        from pydough.metadata.properties import (
            TableColumnMetadata,
            SimpleJoinMetadata,
            CompoundRelationshipMetadata,
            CartesianProductMetadata,
        )

        # Create the string used to identify the property in error messages.
        error_name = f"property {property_name!r} of collection {collection.error_name}"

        # Ensure that the property's name is valid and that the JSON has the
        # required `type` field.
        verify_valid_name(property_name)
        verify_json_has_property_with_type(property_json, "type", str, error_name)

        # Dispatch to each implementation's verification method based on the type.
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
        Parse the JSON describing the metadata for a property within a
        collection to create the property and insert into the collection. It
        is assumed that `PropertyMetadata.verify_json_metadata` has already
        been invoked on the JSON.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted nto.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        from pydough.metadata.properties import (
            TableColumnMetadata,
            SimpleJoinMetadata,
            CompoundRelationshipMetadata,
            CartesianProductMetadata,
        )

        # Dispatch to a parsing procedure based on the `type` field.
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
