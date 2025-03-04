# Unqualified

This subdirectory of the PyDough directory deals with the definitions of unqualified nodes, the transformation of raw code into unqualified nodes, and the conversion from them into PyDough QDAG nodes.

## Unqualified Nodes

Unqualified nodes are the first intermediate representation (IR) created by PyDough whenever a user writes PyDough code. They represent the raw structure of the code before it has been fully qualified into PyDough QDAG nodes. Each unqualified node has a `_parcel` attribute that stores a tuple of its core data fields. Any other `getattr` access on an unqualified node returns another unqualified node, allowing for the dynamic creation of unqualified nodes based on attribute access.

### Available APIs

- `UnqualifiedNode`: The base class for all unqualified nodes.
- `UnqualifiedRoot`: Represents the root of an unqualified node tree.
- `UnqualifiedLiteral`: Represents a literal value in an unqualified node tree.
- `UnqualifiedAccess`: Represents accessing a property from another unqualified node.
- `UnqualifiedCalculate`: Represents a CALCULATE clause being done onto another unqualified node.
- `UnqualifiedWhere`: Represents a WHERE clause being done onto another unqualified node.
- `UnqualifiedOrderBy`: Represents an ORDER BY clause being done onto another unqualified node.
- `UnqualifiedTopK`: Represents a TOP K clause being done onto another unqualified node.
- `UnqualifiedPartition`: Represents a PARTITION clause.
- `UnqualifiedOperation`: Represents any operation done onto 1+ expressions/collections.
- `UnqualifiedBinaryOperation`: Represents a binary operation.
- `UnqualifiedCollation`: Represents a collation expression.
- `UnqualifiedOperator`: Represents a function that has yet to be called.
- `UnqualifiedWindow`: Represents a window operation.

## Code Transformation

The code transformation process rewrites Python code into something legible when there are undefined variables. This is done by transforming raw Python code into PyDough code by replacing undefined variables with unqualified nodes by prepending them with `_ROOT.`.

### Available APIs

- `init_pydough_context`: Decorator that wraps around a PyDough function and transforms its body into unqualified nodes by prepending unknown variables with `_ROOT.`.
- `transform_code`: Transforms the source code into a new Python AST that has had the PyDough decorator removed, had the definition of `_ROOT` injected at the top of the function body, and prepends unknown variables with `_ROOT.`.
- `transform_cell`: Transforms the source code from Jupyter into an updated version with resolved names.

### Usage

To use the code transformation module, you can import the necessary functions and call them with the appropriate arguments. For example:

```python
from pydough.unqualified import init_pydough_context, transform_code, transform_cell
from pydough.metadata import parse_json_metadata_from_file

# Define a graph metadata object by reading from a JSON file
graph = parse_json_metadata_from_file("path/to/metadata.json", "example_graph")

# Define a function with the init_pydough_context decorator
@init_pydough_context(graph)
def example_function():
    return Nations.CALCULATE(
        nation_name=name,
        region_name=region.name,
        num_customers=COUNT(customers)
    )

# Transform the source code of the function
source_code = """
def example_function():
    return Nations.CALCULATE(
        nation_name=name,
        region_name=region.name,
        num_customers=COUNT(customers)
    )
"""
graph_dict = {"example_graph": graph}
known_names = set()
transformed_ast = transform_code(source_code, graph_dict, known_names)

# Display the transformed Python code
import ast
print(ast.unparse(transformed_ast))

# Transform a Jupyter cell
cell_code = """
result = Nations.CALCULATE(
    nation_name=name,
    region_name=region.name,
    num_customers=COUNT(customers)
)
"""
transformed_cell = transform_cell(cell_code, "example_graph", known_names)

# Display the transformed Python code
print(transformed_cell)
```

The transformed Python code for the function will look like this:

```python
from pydough.unqualified import UnqualifiedRoot
_ROOT = UnqualifiedRoot(example_graph)

def example_function():
    return _ROOT.Nations.CALCULATE(
        nation_name=_ROOT.name,
        region_name=_ROOT.region.name,
        num_customers=_ROOT.COUNT(_ROOT.customers)
    )
```

The transformed Python code for the Jupyter cell will look like this:

```python
from pydough.unqualified import UnqualifiedRoot
_ROOT = UnqualifiedRoot(example_graph)

result = _ROOT.Nations.CALCULATE(
    nation_name=_ROOT.name,
    region_name=_ROOT.region.name,
    num_customers=_ROOT.COUNT(_ROOT.customers)
)
```

## Qualification Process

The qualification process transforms unqualified nodes into PyDough QDAG nodes. This involves converting the raw structure of the code into a fully qualified representation that can be used for further processing.

### Available APIs

- `qualify_node`: Transforms an unqualified node into a qualified node.
- `qualify_term`: Transforms an unqualified node into a qualified node within the context of a collection.

### Usage

To use the qualification module, you can import the necessary functions and call them with the appropriate arguments. For example:

```python
import pydough
from pydough.configs import PyDoughConfigs
from pydough.unqualified import qualify_node, qualify_term
from pydough.metadata import parse_json_metadata_from_file
from pydough.unqualified.unqualified_node import UnqualifiedLiteral, UnqualifiedRoot

# Define a graph metadata object by reading from a JSON file
graph = parse_json_metadata_from_file("path/to/metadata.json", "example_graph")

# Define an unqualified node for a collection
unqualified_node = UnqualifiedRoot(graph).Nations.WHERE(region.name == "ASIA")

# Get the active session config
config = pydough.active_session.config

# Qualify the unqualified node
qualified_node = qualify_node(unqualified_node, graph, config)
print(qualified_node.to_string())

# Define a collection context
collection_context = qualified_node

# Qualify a term within the context of the collection
term = UnqualifiedRoot(graph).name
children, qualified_term = qualify_term(collection_context, term, graph, config)
print(qualified_term.to_string())
```

The qualified string form of the `qualified_node` will look like this:

```
Nations.WHERE(region.name == "ASIA")
```

The qualified string form of the `qualified_term` will just be `name` because it is an expression.
