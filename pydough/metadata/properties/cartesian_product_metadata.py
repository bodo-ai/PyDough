"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple
from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata
from pydough.metadata.errors import verify_typ_in_json, verify_typ_in_object


class CartesianProductMetadata(ReversiblePropertyMetadata):
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

    def components(self) -> Tuple:
        return super().components() + (self.other_collection_name,)

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        error_name = f"cartesian product property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_typ_in_json(property_json, "other_collection_name", str, error_name)

    def verify_ready_to_add(self, collection) -> None:
        from pydough.metadata.collections import CollectionMetadata

        super().verify_ready_to_add(collection)
        error_name = f"{self.__class__.__name__} instance {self.name}"
        verify_typ_in_object(self, "other_collection_name", str, error_name)
        verify_typ_in_object(self, "collection", CollectionMetadata, error_name)
        verify_typ_in_object(self, "reverse_collection", CollectionMetadata, error_name)
        verify_typ_in_object(self, "reverse_relationship_name", str, error_name)
        verify_typ_in_object(self, "reverse_property", PropertyMetadata, error_name)

    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        property_json = graph_json[self.collection_name]["properties"][self.name]
        self.other_collection_name = property_json["other_collection_name"]
        self.reverse_relationship_name = property_json["reverse_relationship_name"]

        verify_typ_in_json(
            graph_json, self.collection_name, dict, f"graph {repr(self.graph_name)}"
        )
        self.collection = collections[self.collection_name]

        verify_typ_in_json(
            graph_json,
            self.other_collection_name,
            dict,
            f"graph {repr(self.graph_name)}",
        )
        self.reverse_collection = collections[self.other_collection_name]

        self.build_reverse_relationship()

    def build_reverse_relationship(self) -> ReversiblePropertyMetadata:
        reverse = CartesianProductMetadata(
            self.graph_name, self.other_collection_name, self.reverse_relationship_name
        )
        reverse.reverse_relationship_name = self.name
        reverse.other_collection_name = self.collection_name
        reverse.collection = self.reverse_collection
        reverse.reverse_collection = self.collection
        reverse.reverse_property = self
        self.reverse_property = reverse
