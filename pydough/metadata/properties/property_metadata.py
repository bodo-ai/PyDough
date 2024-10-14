from typing import Dict, Tuple
from pydough.metadata.errors import (
    verify_string_in_json,
    verify_valid_name,
)


class PropertyMetadata(object):
    """
    TODO: add class docstring
    """

    def __init__(self, graph_name: str, collection_name: str, name: str):
        self.graph_name = graph_name
        self.collection_name = collection_name
        self.name = name

    def components(self) -> Tuple:
        """
        TODO: add function doscstring.
        """
        return (self.graph_name, self.collection_name, self.name)

    def __repr__(self):
        return f"{type(self)}({','.join(repr(component) for component in self.components())})"

    def __eq__(self, other):
        return (type(self) is type(other)) and (self.components() == other.components())

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, properties_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        verify_valid_name(collection_name)
        error_name = f"property {property_name} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_string_in_json(properties_json, "type", error_name)
