# Exploration

This subdirectory of the PyDough directory deals with the exploration of PyDough metadata graphs. It provides tools and utilities to navigate, query, and analyze the structure and contents of metadata graphs.

## Available APIs

The exploration module has the following notable APIs available for use:

- `explain`: A function that provides detailed explanations of PyDough metadata objects and unqualified nodes.
- `explain_structure`: A function that provides detailed explanations about the overall structure of a PyDough metadata graph.
- `explain_term`: A function that provides detailed explanations of PyDough unqualified nodes within the context of another PyDough unqualified node.
- `explain_llm`: A function that returns a structured description of a PyDough collection expression for LLM consumption. Returns a JSON-serialisable dict by default (`format="json"`) or a markdown string (`format="md"`) suitable for use in LLM judge prompts.

The APIs take an optional `session` argument which can be used to specify the PyDough session to use for the exploration.

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

## [`explain_llm`](explain_llm.py)

The `explain_llm` function returns a structured description of a PyDough collection expression. Unlike `explain`, which returns human-readable prose, `explain_llm` is designed for programmatic consumption — particularly by LLMs that need to validate, diff, or self-correct generated PyDough code.

The output always has a consistent shape:

```python
# Success
{
    "error": False,
    "query_summary": "...",  # deterministic plain-English sentence
    "steps": [...],          # ordered list of operation steps
    "schema": {...}          # source collection, output columns, ordering, limit
}

# Failure (e.g. unrecognised term, expression instead of collection)
{
    "error": True,
    "message": "Unrecognised term 'typo'. Did you mean: name?",
    "steps": [],
    "schema": None
}
```

Each entry in `steps` represents one operation in execution order (earliest first). Every step includes:
- `order` — 1-based position
- `type` — stable string tag (`"GlobalContext"`, `"TableCollection"`, `"Cross"`, `"SubCollection"`, `"Where"`, `"Calculate"`, `"OrderBy"`, `"TopK"`, `"PartitionBy"`, `"PartitionChild"`, `"Singular"`, `"UserGeneratedCollection"`)
- `description` — short, stable phrase describing what the step does
- `debug` — `{"available_terms": {"expressions": [...], "collections": [...]}}` — scope information separated from the main payload so LLM judges are not distracted by fields irrelevant to correctness
- `notes` — list of strings; always present, may be empty. CROSS steps note both collection names; Calculate steps emit informational notes for implicit relationship-navigation scoping and warnings for potentially unscoped aggregations

The `schema` section includes:
- `source_collection` — root table name, or `null` for graph-level expressions
- `available_expressions` — expression names in scope at the final step
- `output_columns` — explicitly computed columns (only non-empty when the expression ends with `CALCULATE`)
- `column_types` — map of column name → PyDough type string (e.g. `"string"`, `"numeric"`)
- `ordering` — list of `{"text": ..., "direction": "ASC"|"DESC", "nulls": "FIRST"|"LAST"}` from the last `ORDER_BY` or `TOP_K`; `[]` when absent
- `limit` — record limit from `TOP_K`; `null` otherwise

### Usage

```python
from pydough.exploration import explain_llm
from pydough.metadata import parse_json_metadata_from_file

graph = parse_json_metadata_from_file("path/to/metadata.json", "example_graph")

@pydough.init_pydough_context(graph)
def my_query():
    return nations.WHERE(region.name == "ASIA").CALCULATE(key, name)

node = my_query()

# JSON dict — for pipeline machinery
result = explain_llm(node)
# result["error"] is False
# result["steps"] has 4 entries: GlobalContext -> TableCollection -> Where -> Calculate
# result["schema"]["output_columns"] == ["key", "name"]

# Markdown string — for LLM judge prompts
md = explain_llm(node, format="md")
print(md)
```

## Detailed Explanation

The exploration module provides a set of tools and utilities to navigate, query, and analyze the structure and contents of PyDough metadata graphs. It allows users to explore collections, properties, and relationships within the graph, and execute queries to retrieve specific information.

The `explain` function is the main entry point for exploring metadata objects and unqualified nodes. It provides detailed explanations of graphs, collections, and properties, as well as unqualified nodes that can be qualified as collections. The `explain_structure` function provides a high-level overview of the structure of a metadata graph, including the collections and properties it contains. The `explain_term` function provides detailed explanations of unqualified nodes within the context of another unqualified node, helping users understand the meaning and usage of terms within collections.

The `explain_llm` function serves a different purpose from the others: rather than producing human-readable prose, it returns either a structured JSON-serialisable dict (`format="json"`, the default) or a markdown string (`format="md"`). The JSON form is suited for programmatic pipelines — stable keys, diffable, parseable. The markdown form renders the same information in a format that is easier for an LLM judge to read in a prompt. It complements `explain` rather than replacing it — use `explain` for human debugging, `explain_llm` for automated pipelines.
