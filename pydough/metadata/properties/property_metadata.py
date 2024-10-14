from typing import Dict
from pydough.metadata.errors import PyDoughMetadataException


class PropertyMetadata(object):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str):
        self.name = name

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, properties_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        property_json = properties_json[property_name]
        error_name = f"property {property_name} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        if not isinstance(property_json, dict):
            raise PyDoughMetadataException(
                f"Property 'properties' of PyDough {error_name} must be a JSON object."
            )

        if "type" not in property_json:
            raise PyDoughMetadataException(
                f"Metadata for {error_name} missing required property 'type'."
            )
        if not isinstance(property_json["type"], str):
            raise PyDoughMetadataException(
                f"Property 'type' of {error_name} must be a string."
            )
