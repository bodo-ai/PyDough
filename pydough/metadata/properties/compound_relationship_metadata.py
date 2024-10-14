"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple
from .property_metadata import PropertyMetadata
from pydough.metadata.errors import (
    verify_typ_in_json,
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
        self.primary_subcollection_name = None
        self.secondary_subcollection_name = None
        self.inherited_properties_mapping = None

    def components(self) -> Tuple:
        return super().components() + (
            self.primary_subcollection_name,
            self.secondary_subcollection_name,
            self.inherited_properties_mapping,
        )

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"compound relationship property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_typ_in_json(property_json, "primary_subcollection", str, error_name)
        verify_typ_in_json(property_json, "secondary_subcollection", str, error_name)
        verify_typ_in_json(property_json, "reverse_relationship_name", str, error_name)
        verify_typ_in_json(property_json, "singular", bool, error_name)
        verify_typ_in_json(property_json, "no_collisions", bool, error_name)
        verify_json_string_mapping_in_json(
            property_json, "inherited_properties", error_name
        )

    def verify_ready_to_add(self, collection) -> None:
        super().verify_ready_to_add(collection)
        raise NotImplementedError

    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        raise NotImplementedError
