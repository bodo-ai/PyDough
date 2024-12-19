# Metadata

This directory of PyDough deals with the definition, creation, and handling of PyDough metadata.

There are three major classifications of metadata objects in PyDough:
- [Graphs](graphs/README.md)
- [Collections](collections/README.md)
- [Properties](properties/README.md)

## Ways to Create PyDough Metadata

Currently, the only way to create metadata is to store it in a JSON file that is parsed by `parse_json_metadata_from_file`. This function takes in the path to the JSON file and a name of a graph. The JSON file should contain a JSON object whose keys are the names of metadata graphs store in the JSON file and whose values are the JSON objects describing those graphs. The graph name argument should be one of the keys in the JSON file corresponding to the chosen graph to load. The function will parse and verify the appropriate section of metadata from the JSON file corresponding to the requested graph.

TODO: add link to documentation for the specification of the JSON file format for PyDough metadata.
