# Metadata

This directory of PyDough deals with the definition, creation, and handling of PyDough metadata.

There are three major classifications of metadata objects in PyDough, each of which has its own sub-module of the PyDough metadata module:
- [Graphs](graphs/README.md)
- [Collections](collections/README.md)
- [Properties](properties/README.md)

All three of these metadata classifications inherit from the abstract base class `AbstractMetadata`, defined in [abstract_metadata.py](abstract_metadata.py).

Various verification and error-handling utilities used by this module are defined in [error.py](error.py).

## Ways to Create PyDough Metadata

Currently, there are two ways to create metadata in PyDough:

### 1. From a JSON File

The primary method is to store metadata in a JSON file that is parsed by `parse_json_metadata_from_file`. This function takes in the path to the JSON file and a name of a graph. The JSON file should contain a JSON array whose elements are JSON objects representing metadata graphs. The graph name argument should match the "name" field of one of the graph objects in the array. The function will parse and verify the appropriate section of metadata from the JSON file corresponding to the requested graph.

### 2. From a JSON List

For cases where metadata is already loaded into memory (e.g., from an API, database, or string), you can use `parse_metadata_from_list`. This function takes in a Python list of JSON objects (as dictionaries) and a graph name, performing the same validation and graph construction logic as the file-based approach. This is useful when you need to parse metadata from sources other than files, or when you want to cache and reuse metadata without repeatedly reading from disk.

### Implementation Details

Both parsing functions rely on a shared helper function `parse_metadata()` that contains the core logic for iterating through graph objects, validating their structure, and constructing the appropriate `GraphMetadata` instance. The file-based function handles reading and parsing the JSON file, while the list-based function works directly with in-memory data.

The implementation of these functions can be found in [parse.py](abstract_metadata.py).

For the complete specification of the JSON format for PyDough metadata graphs, [see here](../../documentation/metadata.md).