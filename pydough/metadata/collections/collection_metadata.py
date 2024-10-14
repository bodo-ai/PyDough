from typing import List, Union, Dict, Tuple
from pydough.metadata.errors import (
    PyDoughMetadataException,
    verify_string_in_json,
    verify_json_in_json,
    verify_valid_name,
)
from pydough.metadata.properties import PropertyMetadata, InheritedPropertyMetadata


class CollectionMetadata(object):
    """
    TODO: add class docstring
    """

    allowed_properties = ["type", "properties"]

    def __init__(self, graph_name: str, name: str):
        self.graph_name: str = graph_name
        self.name: str = name
        self.properties: Dict[str, PropertyMetadata] = {}
        self.inherited_properties: Dict[str, Tuple[InheritedPropertyMetadata]] = {}

    def components(self) -> Tuple:
        """
        TODO: add function doscstring.
        """
        return (self.graph_name, self.name)

    def __repr__(self):
        return f"{type(self)}({','.join(repr(component) for component in self.components())})"

    def __eq__(self, other):
        return (type(self) is type(other)) and (self.components() == other.components())

    def verify_json_metadata(
        graph_name: str, collection_name: str, graph_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        from pydough.metadata.collections import SimpleTableMetadata

        verify_valid_name(collection_name)
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

        properties_json = collection_json["properties"]
        for property_name in collection_json["properties"]:
            verify_json_in_json(properties_json, property_name, error_name)
            PropertyMetadata.verify_json_metadata(
                graph_name,
                collection_name,
                property_name,
                properties_json[property_name],
            )

    def parse_from_json(self, graph_json: dict) -> None:
        """
        TODO: add function doscstring.
        """
        raise NotImplementedError(
            f"Collection metadata class {type(PyDoughMetadataException).__name__} does not have a parse_from_json method implemented"
        )

    def get_nouns(
        self,
    ) -> List[Tuple[str, Union["CollectionMetadata", PropertyMetadata]]]:
        """
        TODO: add function doscstring.
        """
        nouns = [(self.name, self)]
        for property in self.properties.values():
            nouns.append((property.name, property))
        for property in self.inherited_properties.values():
            nouns.append((property.name, property))
        return nouns

    def get_property_names(self) -> List[str]:
        """
        TODO: add function doscstring.
        """
        return list(self.properties)

    def get_property(self, property_name: str) -> PropertyMetadata:
        """
        TODO: add function doscstring.
        """
        if property_name not in self.properties:
            raise PyDoughMetadataException(
                f"Collection {self.name} does not have a property {repr(property_name)}"
            )
        return self.properties[property_name]
