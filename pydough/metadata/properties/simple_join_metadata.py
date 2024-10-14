"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple
from .property_metadata import PropertyMetadata
from pydough.metadata.errors import (
    verify_string_in_json,
    verify_boolean_in_json,
    verify_json_string_mapping_in_json,
)


class SimpleJoinMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        graph_name: str,
        collection_name: str,
        name: str,
    ):
        super().__init__(graph_name, collection_name, name)
        self.other_collection_name = None
        self.singular = None
        self.no_collisions = None
        self.keys = None
        self.reverse_relationship_name = None

    def components(self) -> Tuple:
        return super().components() + (
            self.other_collection_name,
            self.singular,
            self.no_collisions,
            self.keys,
            self.reverse_relationship_name,
        )

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"simple join property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_string_in_json(property_json, "other_collection_name", error_name)
        verify_boolean_in_json(property_json, "singular", error_name)
        verify_boolean_in_json(property_json, "no_collisions", error_name)
        verify_json_string_mapping_in_json(property_json, "keys", error_name)
        verify_string_in_json(property_json, "reverse_relationship_name", error_name)

    def parse_from_json(self, graph_json: dict) -> None:
        raise NotImplementedError
