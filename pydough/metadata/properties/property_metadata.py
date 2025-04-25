"""
Definition of the base class for PyDough metadata for a properties.
"""

__all__ = ["PropertyMetadata"]

from abc import abstractmethod

from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    HasType,
    PyDoughMetadataException,
    is_string,
    is_valid_name,
)


class PropertyMetadata(AbstractMetadata):
    """
    Abstract base class for PyDough metadata for properties.

    Each implementation must include the following APIs:
    - `create_error_name`
    - `components`
    - `is_plural`
    - `is_subcollection`
    - `is_reversible`
    - `verify_json_metadata`
    - `parse_from_json`
    """

    # Set of names of fields that can be included in the JSON object
    # describing a property. Implementations should extend this.
    allowed_fields: set[str] = {"type"}

    def __init__(self, name: str, collection: CollectionMetadata):
        is_valid_name.verify(name, "name")
        HasType(CollectionMetadata).verify(collection, "collection")
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

    @property
    def path(self) -> str:
        return f"{self.collection.path}.{self.name}"

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
    def components(self) -> list:
        comp: list = self.collection.components
        comp.append(self.name)
        return comp

    @staticmethod
    def get_class_for_property_type(
        name: str, error_name: str
    ) -> type["PropertyMetadata"]:
        """
        Fetches the PropertyType implementation class for a string
        representation of the property type.

        Args:
            `name`: the string representation of a property type.
            `error_name`: the string used in error messages to describe
            the object that `name` came from.

        Returns:
            The class of the property type corresponding to `name`.

        Raises:
            `PyDoughMetadataException` if the string does not correspond
            to a known property type.
        """
        from pydough.metadata.properties import (
            CartesianProductMetadata,
            CompoundRelationshipMetadata,
            GeneralJoinMetadata,
            SimpleJoinMetadata,
            TableColumnMetadata,
        )

        match name:
            case "table_column":
                return TableColumnMetadata
            case "simple_join":
                return SimpleJoinMetadata
            case "general_join":
                return GeneralJoinMetadata
            case "cartesian_product":
                return CartesianProductMetadata
            case "compound":
                return CompoundRelationshipMetadata
            case property_type:
                raise PyDoughMetadataException(
                    f"Unrecognized property type for {error_name}: {repr(property_type)}"
                )

    @staticmethod
    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Verifies that the JSON describing the metadata for a property within
        a collection is well-formed before parsing it to create the property
        and insert into the collection.

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
        error_name = f"property {property_name!r} of {collection.error_name}"

        # Ensure that the property's name is valid and that the JSON has the
        # required `type` field.
        is_valid_name.verify(property_name, "property name")
        HasPropertyWith("type", is_string).verify(property_json, error_name)

        # Dispatch to each implementation's verification method based on the type.
        property_class = PropertyMetadata.get_class_for_property_type(
            property_json["type"], error_name
        )
        property_class.verify_json_metadata(collection, property_name, property_json)

    @staticmethod
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
            property would be inserted into.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Create the string used to identify the property in error messages.
        error_name = f"property {property_name!r} of {collection.error_name}"

        # Dispatch to each implementation's parseing method based on the type.
        property_class: type[PropertyMetadata] = (
            PropertyMetadata.get_class_for_property_type(
                property_json["type"], error_name
            )
        )
        property_class.parse_from_json(collection, property_name, property_json)

    def get_nouns(self) -> dict[str, list[AbstractMetadata]]:
        return {self.name: [self]}
