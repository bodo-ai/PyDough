# Metadata Graphs

This subdirectory of the PyDough metadata directory deals with the definition of PyDough metadata graphs.

A graph in PyDough is a collection of collections linked together via a network of properties defining subcollection relationships. The definition of the graph class is found in [graph_metadata.py](graph_metadata.py).

## Available APIs

The graph metadata has the following notable APIs available for use, in addition to the standard APIs for all metadata objects:
- `name`: a property of the graph metadata storing the name of the graph.
- `get_collection_names`: returns a set of the names of all collections in the graph.
- `get_collection`: takes in the name of a collection in the graph and returns the metadata for that collection, or raises an error if there is no collection of that name in the graph.
    - Can index into the graph with a string of a collection name, as if it were a dictionary, to do the same thing.
