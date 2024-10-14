from typing import Dict, Tuple
from pydough.metadata.errors import PyDoughMetadataException
from .property_metadata import PropertyMetadata


class InheritedPropertyMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        parent_collection,
        parent_property: PropertyMetadata,
    ):
        super().__init__(name)
        from pydough.metadata.collections import CollectionMetadata

        if not isinstance(parent_collection, CollectionMetadata):
            raise PyDoughMetadataException(
                f"InheritedPropertyMetadata expects parent collection to be a CollectionMetadata, received: {repr(parent_collection)}"
            )
        if not isinstance(parent_property, PropertyMetadata):
            raise PyDoughMetadataException(
                f"InheritedPropertyMetadata expects parent property to be a PropertyMetadata, received: {repr(parent_property)}"
            )
        self.parent_collection = parent_collection
        self.parent_property = parent_property

    def components(self) -> Tuple:
        """
        TODO: add function doscstring.
        """
        return super().components() + (self.parent_collection, self.parent_property)

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, properties_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        raise NotImplementedError
