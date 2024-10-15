"""
TODO: add file-level docstring
"""

from typing import Dict
from .property_metadata import PropertyMetadata
from pydough.metadata.errors import PyDoughMetadataException


class InheritedPropertyMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        original_collection_name: str,
        parent_collection_name: str,
        parent_property_name: str,
    ):
        super().__init__(name)
        self.original_collection_name = original_collection_name
        self.parent_collection_name = parent_collection_name
        self.parent_property_name = parent_property_name

    def components(self) -> tuple:
        return super().components() + (
            self.original_collection_name,
            self.parent_collection_name,
            self.parent_property_name,
        )

    def verify_ready_to_add(self, collection) -> None:
        super().verify_ready_to_add(collection)
        raise NotImplementedError

    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        raise PyDoughMetadataException(
            "Cannot directly construct an instance of InheritedPropertyMetadata from JSON"
        )
