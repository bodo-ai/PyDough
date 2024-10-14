from typing import List, Union, Dict, Tuple
from collections import defaultdict

class PyDoughMetadataException(Exception):
    """
    TODO: add class docstring
    """

class PropertyMetadata(object):
    """
    TODO: add class docstring
    """
    def __init__(self, name: str):
        self.name = name

class CollectionMetadata(object):
    """
    TODO: add class docstring
    """
    def __init__(self, name: str):
        self.name = name

    def verify_metadata(graph_name: str, collection_name: str, collection_json: Dict) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"collection {repr(collection_name)} in graph {repr(graph_name)}"
        if "properties" not in collection_json:
            raise PyDoughMetadataException(f"Metadata for {error_name} missing required property 'properties'.")
        if not isinstance(collection_json["properties"], dict):
            raise PyDoughMetadataException(f"Property 'properties' of PyDough {error_name} must be a JSON object.")

    def get_nouns(self) -> List[Tuple[str, Union["CollectionMetadata", PropertyMetadata]]]:
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
    
class SimpleTableMetadata(CollectionMetadata):
    """
    TODO: add class docstring
    """
    
    def verify_metadata(graph_name: str, collection_name: str, collection_json: Dict) -> None:
        """
        TODO: add function doscstring.
        """
        CollectionMetadata.verify_metadata(graph_name, collection_name, collection_json)
        
        error_name = f"simple table collection {repr(collection_name)} in graph {repr(graph_name)}"
        
        if "table_path" not in collection_json:
            raise PyDoughMetadataException(f"Metadata for {error_name} missing required property 'table_path'.")
        if not isinstance(collection_json["table_path"], str):
            raise PyDoughMetadataException(f"Property 'table_path' of {error_name} must be a string.")
        
        if "unique_properties" not in collection_json:
            raise PyDoughMetadataException(f"Metadata for {error_name} missing required property 'table_path'.")
        if not isinstance(collection_json["unique_properties"], list) or len(collection_json["unique_properties"]) == 0 or not all(
            isinstance(elem, str) or 
            (isinstance(elem, list) and all(isinstance(sub_elem, str) for sub_elem in elem))
            for elem in collection_json["unique_properties"]
        ):
            raise PyDoughMetadataException(f"Property 'unique_properties' of {error_name} must be a non-empty list whose elements are all either strings or lists of strings.")
        

class GraphMetadata(object):
    """
    TODO: add class docstring
    """
    def __init__(self, name : str, collections : Dict[str, CollectionMetadata]):
        self.name : str = name
        self.collections : Dict[str, CollectionMetadata] = collections
        self.nouns : Dict[str, List[Union[CollectionMetadata, PropertyMetadata]]] = defaultdict(list)
        self.nouns[self.name].append(self)
        for collection in self.collections.values():
            for noun, value in collection.get_nouns():
                self.nouns[noun].append(value) 

    def get_collection_names(self) -> List[str]:
        """
        Fetches all of the names of collections in the current graph.
        """
        return list(self.collections)
    
    def get_collection(self, collection_name: str) -> CollectionMetadata:
        """
        Fetches a specific collection's metadata from within the graph by name.
        """
        if collection_name not in self.collections:
            raise Exception(f"Graph {self.name} does not have a collection named {collection_name}")
        return self.collections[collection_name]
    
    def get_nouns(self) -> Dict[str, List[Union[CollectionMetadata, PropertyMetadata]]]:
        """
        Fetches all of the names of collections/properties in the graph.
        """
        return self.nouns

