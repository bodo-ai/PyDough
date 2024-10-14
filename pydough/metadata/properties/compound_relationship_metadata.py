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


class CompoundRelationshipMetadata(PropertyMetadata):
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
        self.primary_subcollection = None
        self.secondary_subcollection = None
        self.inherited_properties = None

    def components(self) -> Tuple:
        """
        TODO: add function doscstring.
        """
        return super().components() + (self.column_name, self.data_type)

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"compound relationship property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_string_in_json(property_json, "primary_subcollection", error_name)
        verify_string_in_json(property_json, "secondary_subcollection", error_name)
        verify_string_in_json(property_json, "reverse_relationship_name", error_name)
        verify_boolean_in_json(property_json, "singular", error_name)
        verify_boolean_in_json(property_json, "no_collisions", error_name)
        verify_json_string_mapping_in_json(
            property_json, "inherited_properties", error_name
        )

    def parse_from_json(self, graph_json: dict) -> None:
        """
        TODO: add function doscstring.
        """
        pass
