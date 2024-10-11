from typing import List, Union, Dict, Tuple
from collections import defaultdict

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

    def get_nouns(self) -> List[Tuple[str, Union["CollectionMetadata", PropertyMetadata]]]:
        nouns = [(self.name, self)]
        return nouns

    def get_property_names(self) -> List[str]:
        return [property.name for property in self.properties]

    def get_property(self) -> List[str]:
        return [property.name for property in self.properties]
    
    def solidify_properties(self, graph: "GraphMetadata") -> None:
        return

class GraphMetadata(object):
    """
    TODO: add class docstring
    """
    def __init__(self, name : str, collections : List[CollectionMetadata]):
        self.name : str = name
        self.collections : Dict[str, CollectionMetadata] = {collection.name: collection for collection in collections}
        self.nouns : Dict[str, List[Union[CollectionMetadata, PropertyMetadata]]] = defaultdict(list)
        for collection in self.collections.values():
            collection.solidify_properties(self)
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

