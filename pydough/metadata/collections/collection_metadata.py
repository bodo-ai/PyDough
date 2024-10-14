from typing import List, Union, Dict, Tuple
from pydough.metadata.errors import (
    PyDoughMetadataException,
    verify_string_in_json,
    verify_json_in_json,
)
from pydough.metadata.collections import SimpleTableMetadata
from pydough.metadata.properties import PropertyMetadata


class CollectionMetadata(object):
    """
    TODO: add class docstring
    """

    allowed_properties = ["type", "properties"]

    def __init__(self, name: str):
        self.name = name

    def verify_json_metadata(
        graph_name: str, collection_name: str, graph_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        collection_json = graph_json[collection_name]
        error_name = f"collection {repr(collection_name)} in graph {repr(graph_name)}"
        if collection_name == graph_name:
            raise PyDoughMetadataException(
                f"Cannot have collection named {repr(collection_name)} share the same name as the graph containing it."
            )

        verify_string_in_json(collection_json, "type", error_name)
        verify_json_in_json(collection_json, "properties", error_name)

        match collection_json["type"]:
            case "simple_table":
                SimpleTableMetadata.verify_json_metadata(
                    graph_name, collection_name, collection_json
                )
            case collection_type:
                raise PyDoughMetadataException(
                    f"Unrecognized collection type for {error_name}: {repr(collection_type)}"
                )

    def get_nouns(
        self,
    ) -> List[Tuple[str, Union["CollectionMetadata", PropertyMetadata]]]:
        """
        TODO: add function doscstring.
        """
        nouns = [(self.name, self)]
        return nouns

    def get_property_names(self) -> List[str]:
        """
        TODO: add function doscstring.
        """
        return [property.name for property in self.properties]

    def get_property(self) -> List[str]:
        """
        TODO: add function doscstring.
        """
        return [property.name for property in self.properties]
