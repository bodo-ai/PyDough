# Exploration

This subdirectory of the PyDough directory deals with the exploration of PyDough metadata graphs. It provides tools and utilities to navigate, query, and analyze the structure and contents of metadata graphs.

## Available APIs

The exploration module has the following notable APIs available for use:

- `explain`: A function that provides detailed explanations of PyDough metadata objects and unqualified nodes.
- `explain_structure`: A function that provides detailed explanations about the overall structure of a PyDough metadata graph.
- `explain_term`: A function that provides detailed explanations of PyDough unqualified nodes within the context of another PyDough unqualified node.

The APIs take an optional `config` argument which can be used to specify the PyDough configuration settings to use for the exploration.

## [explain](explain.py)

The `explain` function displays information about a PyDough metadata object or unqualified node. The metadata could be for a graph, collection, or property. An unqualified node can only be passed in if it is possible to qualify it as a PyDough collection. If not, then `explain_term` may need to be used.

### Usage

To use the `explain` function, you can import it and call it with a metadata object or unqualified node. For example:

```python
from pydough.exploration import explain
from pydough.metadata import parse_json_metadata_from_file

# Define a graph metadata object by reading from a JSON file
graph = parse_json_metadata_from_file("path/to/metadata.json", "example_graph")

# Explain the graph
print(explain(graph, verbose=True))

# Explain a collection within the graph
collection = graph.get_collection("Nations")
print(explain(collection, verbose=True))

# Explain a property within the collection
property = collection.get_property("name")
print(explain(property, verbose=True))
```

The output for explaining the graph will look like this:

```
PyDough graph: example_graph
Collections: graph contains no collections
```

The output for explaining the collection will look like this:

```
PyDough collection: Nations
Table path: tpch.NATION
Unique properties of collection: ['key']
Scalar properties:
  comment
  key
  name
  region_key
Subcollection properties:
  customers
  orders_shipped_to
  region
  suppliers
Call pydough.explain(graph['Nations'][property_name]) to learn more about any of these properties.
```

The output for explaining the property will look like this:

```
PyDough property: Nations.name
Column name: tpch.NATION.n_name
Data type: string
```

## [`explain_structure`](structure.py)

The `explain_structure` function displays information about a PyDough metadata graph, including the names of each collection in the graph, the names of all scalar and subcollection properties for each collection, and the details of subcollection properties.

### Usage

To use the `explain_structure` function, you can import it and call it with a metadata graph. For example:

```python
from pydough.exploration import explain_structure
from pydough.metadata import parse_json_metadata_from_file

# Define a graph metadata object by reading from a JSON file
graph = parse_json_metadata_from_file("path/to/metadata.json", "example_graph")

# Explain the structure of the graph
print(explain_structure(graph))
```

The output for explaining the structure of the graph will look like this:

```
Structure of PyDough graph: example_graph
  Graph contains no collections
```

## [`explain_term`](term.py)

The `explain_term` function displays information about an unqualified node as it exists within the context of another unqualified node. This information can include the structure of the qualified collection and term, additional children of the collection, the meaning of the term within the collection, the cardinality of the term within the collection, examples of how to use the term within the collection, and how to learn more about the term.

### Usage

To use the `explain_term` function, you can import it and call it with an unqualified node and a term. For example:

```python
from pydough.exploration import explain_term
from pydough.metadata import parse_json_metadata_from_file
from pydough.unqualified import UnqualifiedRoot

# Define a graph metadata object by reading from a JSON file
graph = parse_json_metadata_from_file("path/to/metadata.json", "example_graph")

# Define an unqualified node for a collection
unqualified_node = UnqualifiedRoot(graph).Nations.WHERE(region.name == "ASIA")

# Define a term within the context of the collection
term = UnqualifiedRoot(graph).name

# Explain the term within the context of the collection
print(explain_term(unqualified_node, term, verbose=True))
```

The output for explaining the term within the context of the collection will look like this:

```
Collection:
  ──┬─ TPCH
    └─── TableCollection[Nations]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─── SubCollection[region]

The term is the following expression: $1.name

This is a reference to expression 'name' of child $1
```

## Detailed Explanation

The exploration module provides a set of tools and utilities to navigate, query, and analyze the structure and contents of PyDough metadata graphs. It allows users to explore collections, properties, and relationships within the graph, and execute queries to retrieve specific information.

The `explain` function is the main entry point for exploring metadata objects and unqualified nodes. It provides detailed explanations of graphs, collections, and properties, as well as unqualified nodes that can be qualified as collections. The `explain_structure` function provides a high-level overview of the structure of a metadata graph, including the collections and properties it contains. The `explain_term` function provides detailed explanations of unqualified nodes within the context of another unqualified node, helping users understand the meaning and usage of terms within collections.

By using the exploration module, users can gain insights into the structure and contents of PyDough metadata graphs, and perform various analyses and queries to retrieve specific information.
