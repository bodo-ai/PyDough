# Metadata

This directory of PyDough deals with the definition, creation, and handling of PyDough metadata.

There are three major classifications of metadata objects in PyDough, each of which has its own sub-module of the PyDough metadata module:
- [Graphs](graphs/README.md)
- [Collections](collections/README.md)
- [Properties](properties/README.md)

All three of these metadata classifications inherit from the abstract base class `AbstractMetadata`, defined in [abstract_metadata.py](abstract_metadata.py).

Various verification and error-handling utilities used by this module are defined in [error.py](error.py).

## Ways to Create PyDough Metadata

Currently, the only way to create metadata is to store it in a JSON file that is parsed by `parse_json_metadata_from_file`. This function takes in the path to the JSON file and a name of a graph. The JSON file should contain a JSON object whose keys are the names of metadata graphs store in the JSON file and whose values are the JSON objects describing those graphs. The graph name argument should be one of the keys in the JSON file corresponding to the chosen graph to load. The function will parse and verify the appropriate section of metadata from the JSON file corresponding to the requested graph.

The logic for how `parse_json_metadata_from_file` is implemented can be found in [parse.py](abstract_metadata.py).

To see the specification of the JSON file format for PyDough, [see here](../../documentation/metadata.md).
