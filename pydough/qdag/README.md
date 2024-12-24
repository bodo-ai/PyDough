# PyDough QDAG Module

This module deals with the qualified DAG (QDAG) structure used as an intermediary representation after unqualified nodes and before the relational tree. The QDAG nodes represent collections and expressions in a structured manner that can be further processed and transformed.

## Available APIs

The QDAG module provides the following notable APIs:

### Base Classes

- `PyDoughQDAG`: Base class for all QDAG nodes, including collections and expressions.

### Expression Nodes

- `PyDoughExpressionQDAG`: Base class for all expression QDAG nodes.
- `Literal`: Represents a literal value in the QDAG.
- `ColumnProperty`: Represents a column property in a table.
- `ExpressionFunctionCall`: Represents a function call expression.
- `Reference`: Represents a reference to an expression in a collection.
- `ChildReferenceExpression`: Represents a reference to an expression in a child collection.
- `BackReferenceExpression`: Represents a reference to an expression in an ancestor collection.
- `CollationExpression`: Represents a collation expression.

### Collection Nodes

- `PyDoughCollectionQDAG`: Base class for all collection QDAG nodes.
- `GlobalContext`: Represents the global context of the graph.
- `TableCollection`: Represents a table collection.
- `SubCollection`: Represents a subcollection.
- `Calc`: Represents a CALC node.
- `Where`: Represents a WHERE clause.
- `OrderBy`: Represents an ORDER BY clause.
- `TopK`: Represents a TOP K clause.
- `PartitionBy`: Represents a PARTITION BY clause.
- `ChildAccess`: Represents accessing a child collection.
- `ChildReferenceCollection`: Represents a reference to a child collection.
- `BackReferenceCollection`: Represents a reference to an ancestor collection.
- `ChildOperatorChildAccess`: Represents accessing a child collection within a child operator context.

### Errors

- `PyDoughQDAGException`: Exception class for errors related to QDAG nodes.

### Node Builder

- `AstNodeBuilder`: Utility class for building QDAG nodes.

#### Methods

- `build_literal(value: object, data_type: PyDoughType) -> Literal`: Creates a new literal of the specified PyDough type using a passed-in literal value.
- `build_column(collection_name: str, property_name: str) -> ColumnProperty`: Creates a new column property node by accessing a specific property of a collection in the graph by name.
- `build_expression_function_call(function_name: str, args: list[PyDoughQDAG]) -> ExpressionFunctionCall`: Creates a new expression function call by accessing a builtin expression function operator by name and calling it on the passed-in arguments.
- `build_reference(collection: PyDoughCollectionQDAG, name: str) -> Reference`: Creates a new reference to an expression from a preceding collection.
- `build_child_reference_expression(children: MutableSequence[PyDoughCollectionQDAG], child_idx: int, name: str) -> Reference`: Creates a new reference to an expression from a child collection of a CALC.
- `build_back_reference_expression(collection: PyDoughCollectionQDAG, name: str, levels: int) -> Reference`: Creates a new reference to an expression from an ancestor collection.
- `build_global_context() -> GlobalContext`: Creates a new global context for the graph.
- `build_child_access(name: str, preceding_context: PyDoughCollectionQDAG) -> ChildAccess`: Creates a new child access QDAG node.
- `build_calc(preceding_context: PyDoughCollectionQDAG, children: MutableSequence[PyDoughCollectionQDAG]) -> Calc`: Creates a CALC instance, but `with_terms` still needs to be called on the output.
- `build_where(preceding_context: PyDoughCollectionQDAG, children: MutableSequence[PyDoughCollectionQDAG]) -> Where`: Creates a WHERE instance, but `with_condition` still needs to be called on the output.
- `build_order(preceding_context: PyDoughCollectionQDAG, children: MutableSequence[PyDoughCollectionQDAG]) -> OrderBy`: Creates an ORDERBY instance, but `with_collation` still needs to be called on the output.
- `build_top_k(preceding_context: PyDoughCollectionQDAG, children: MutableSequence[PyDoughCollectionQDAG], records_to_keep: int) -> TopK`: Creates a TOP K instance, but `with_collation` still needs to be called on the output.
- `build_partition(preceding_context: PyDoughCollectionQDAG, child: PyDoughCollectionQDAG, child_name: str) -> PartitionBy`: Creates a PARTITION BY instance, but `with_keys` still needs to be called on the output.
- `build_back_reference_collection(collection: PyDoughCollectionQDAG, term_name: str, back_levels: int) -> BackReferenceCollection`: Creates a reference to a subcollection of an ancestor.
- `build_child_reference_collection(preceding_context: PyDoughCollectionQDAG, children: MutableSequence[PyDoughCollectionQDAG], child_idx: int) -> ChildReferenceCollection`: Creates a new reference to a collection from a child collection of a CALC or other child operator.

## Usage

### Building QDAG Nodes

To build QDAG nodes, use the `AstNodeBuilder` class. For example:

```python
from pydough.qdag import AstNodeBuilder, Literal, Calc
from pydough.metadata import GraphMetadata
from pydough.types import Int64Type

# Define the graph metadata
graph = GraphMetadata(...)

# Create a node builder
builder = AstNodeBuilder(graph)

# Build a literal node
literal_node = builder.build_literal(42, Int64Type())

# Build a column property node
column_node = builder.build_column("Orders", "order_date")

# Build an expression function call node
function_call_node = builder.build_expression_function_call("SUM", [literal_node, column_node])

# Build a reference node
reference_node = builder.build_reference(table_collection, "region")

# Build a child reference expression node
child_reference_node = builder.build_child_reference_expression([table_collection], 0, "name")

# Build a back reference expression node
back_reference_node = builder.build_back_reference_expression(table_collection, "region_key", 1)

# Build a global context node
global_context_node = builder.build_global_context()

# Build a child access node
child_access_node = builder.build_child_access("Orders", global_context_node)

# Build a CALC node
calc_node = builder.build_calc(preceding_context, [])
calc_node = calc_node.with_terms([("result", literal_node)])

# Build a WHERE node
where_node = builder.build_where(table_collection, [])
where_node = where_node.with_condition(condition)

# Build an ORDER BY node
order_by_node = builder.build_order(table_collection, [])
order_by_node = order_by_node.with_collation([collation_expression])

# Build a TOP K node
top_k_node = builder.build_top_k(table_collection, [], 5)
top_k_node = top_k_node.with_collation([collation_expression])

# Build a PARTITION BY node
partition_by_node = builder.build_partition(preceding_context, child_collection, "child_name")
partition_by_node = partition_by_node.with_keys([child_reference_node])

# Build a back reference collection node
back_reference_collection_node = builder.build_back_reference_collection(table_collection, "subcollection", 1)

# Build a child reference collection node
child_reference_collection_node = builder.build_child_reference_collection(preceding_context, [child_collection], 0)
```

### Working with Collection Nodes

Collection nodes represent collections in the QDAG. For example:

```python
from pydough.qdag import AstNodeBuilder, Where, OrderBy, Reference
from pydough.metadata import GraphMetadata

# Define the graph metadata
graph = GraphMetadata(...)

# Create a node builder
builder = AstNodeBuilder(graph)

# Create a table collection node
table_collection = builder.build_child_access("Orders", builder.build_global_context())

# Create a WHERE clause
condition = builder.build_expression_function_call(
    "EQU",
    [Reference(table_collection, "region"), builder.build_literal("ASIA", StringType())]
)
where_clause = builder.build_where(table_collection, [])
where_clause = where_clause.with_condition(condition)

# Create an ORDER BY clause
collation_expression = builder.build_collation_expression(
    Reference(table_collection, "order_date"), True, True
)
order_by_clause = builder.build_order(table_collection, [])
order_by_clause = order_by_clause.with_collation([collation_expression])
```

### Example with Child References

To create a QDAG node with child references, use the `AstNodeBuilder` class. For example:

```python
from pydough.qdag import AstNodeBuilder, Where, ChildReferenceExpression
from pydough.metadata import GraphMetadata
from pydough.types import StringType

# Define the graph metadata
graph = GraphMetadata(...)

# Create a node builder
builder = AstNodeBuilder(graph)

# Create a table collection node
table_collection = builder.build_child_access("Nations", builder.build_global_context())

# Create a WHERE clause with a child reference
region_collection = builder.build_child_access("region", table_collection)
condition = builder.build_expression_function_call(
    "EQU",
    [ChildReferenceExpression(region_collection, 0, "name"), builder.build_literal("ASIA", StringType())]
)
where_clause = builder.build_where(table_collection, [region_collection])
where_clause = where_clause.with_condition(condition)
```

### HAS/HASNOT Rewrite

The `has_hasnot_rewrite` function is used to transform `HAS` and `HASNOT` expressions in the QDAG. It is used in the `with_terms`, `with_condition`, and `with_collation` calls of the various child operator classes to rewrite all `HAS`/`HASNOT` terms unless they meet the criteria.

#### Example

```python
from pydough.qdag import AstNodeBuilder, Where, ChildReferenceExpression, has_hasnot_rewrite
from pydough.metadata import GraphMetadata
from pydough.types import StringType

# Define the graph metadata
graph = GraphMetadata(...)

# Create a node builder
builder = AstNodeBuilder(graph)

# Create a table collection node
table_collection = builder.build_child_access("Nations", builder.build_global_context())

# Create a WHERE clause with a HAS expression
condition = builder.build_expression_function_call(
    "HAS",
    [ChildReferenceExpression(table_collection, 0, "region")]
)
where_clause = builder.build_where(table_collection, [])

# In this case, call `HAS(region)` will be replaced with `COUNT(region) > 0`
# because the `allow_has_hasnot` argument is False.
where_clause = where_clause.with_condition(has_hasnot_rewrite(condition, False))
```

By using these APIs, the QDAG module provides a comprehensive set of tools for building and working with qualified DAG nodes in PyDough.
