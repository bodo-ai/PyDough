"""
Definition of the base class for PyDough metadata for a properties.
"""

__all__ = ["PropertyMetadata"]

from abc import abstractmethod

from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    HasType,
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
    - `parse_from_json`
    """

    # Set of names of fields that can be included in the JSON object
    # describing a property. Implementations should extend this.
    allowed_fields: set[str] = {
        "name",
        "type",
        "description",
        "synonyms",
        "extra semantic info",
    }

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ):
        is_valid_name.verify(name, "name")
        HasType(CollectionMetadata).verify(collection, "collection")
        self._name: str = name
        self._collection: CollectionMetadata = collection
        super().__init__(description, synonyms, extra_semantic_info)

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
