from typing import Dict
from pydough.metadata.errors import (
    verify_string_in_json,
    verify_list_of_string_or_strings_in_json,
    verify_no_extra_keys_in_json,
)
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.properties import PropertyMetadata


class SimpleTableMetadata(CollectionMetadata):
    """
    TODO: add class docstring
    """

    allowed_properties = CollectionMetadata.allowed_properties + [
        "table_path",
        "unique_properties",
    ]

    def verify_json_metadata(
        graph_name: str, collection_name: str, collection_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"simple table collection {repr(collection_name)} in graph {repr(graph_name)}"

        verify_string_in_json(collection_json, "table_path", error_name)
        verify_list_of_string_or_strings_in_json(
            collection_json, "unique_properties", error_name
        )
        verify_no_extra_keys_in_json(
            collection_json, SimpleTableMetadata.allowed_properties, error_name
        )

        for property_name in collection_json["properties"]:
            PropertyMetadata.verify_json_metadata(
                graph_name, collection_name, property_name
            )
