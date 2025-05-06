# Relational Nodes

This subdirectory of the PyDough directory deals with the representation of relational nodes used in the relational tree to build the final SQL query.

The relational_nodes module provides functionality to define and manage various relational nodes that can be used within PyDough.

## Available APIs

### [abstract_node.py](abstract_node.py)

- `Relational`: The abstract base class for all relational nodes. Each implementation class must define the following:
    - `columns`: The columns of the relational node.
    - `inputs`: The inputs to the relational node.
    - `node_equals`: Determines if two relational nodes are exactly identical.
    - `to_string`: Converts the relational node to a string.
    - `accept`: Visits the relational node with the provided visitor.
    - `node_copy`: Creates a copy of the relational node with the specified columns and inputs.

### [relational_root.py](relational_root.py)

- `RelationalRoot`: The root node in any relational tree. This node is responsible for enforcing the final orderings and columns as well as any other traits that impact the shape/display of the final output.

### [single_relational.py](single_relational.py)

- `SingleRelational`: The base abstract class for relational nodes that have a single input, such as `Filter`, `Project`, `Limit`.

### [scan.py](scan.py)

- `Scan`: The relational node representing a base table in the relational tree.

### [filter.py](filter.py)

- `Filter`: The relational node representing a filter operation in the relational tree.

### [project.py](project.py)

- `Project`: The relational node representing a project operation in the relational tree.

### [limit.py](limit.py)

- `Limit`: The relational node representing a limit operation in the relational tree.

### [join.py](join.py)

- `Join`: The relational node representing a join operation in the relational tree.
    - `JoinType`: Enum class used to describe the various types of joins.

### [aggregate.py](aggregate.py)

- `Aggregate`: The relational node representing an aggregation operation in the relational tree.

### [empty_singleton.py](empty_singleton.py)

- `EmptySingleton`: The relational node representing a constant table with 1 row and 0 columns, used as a base case when converting into relational nodes.

### [column_pruner.py](column_pruner.py)

- `ColumnPruner`: Module responsible for pruning columns from relational expressions.

### [relational_visitor.py](relational_visitor.py)

- `RelationalVisitor`: The basic Visitor pattern to perform operations across an entire relational tree.

### [relational_expression_dispatcher.py](relational_expression_dispatcher.py)

- `RelationalExpressionDispatcher`: Implementation of a visitor that works by visiting every expression for each node.

### [tree_string_visitor.py](tree_string_visitor.py)

- `TreeStringVisitor`: Implementation of a visitor that converts relational nodes into a tree string.

### [join_type_relational_visitor.py](join_type_relational_visitor.py)

- `JoinTypeRelationalVisitor`: Implementation of a visitor that collects join types from a relational tree.

## Usage

To use the relational_nodes module, you can import the necessary classes and call them with the appropriate arguments. For example:

```python
from pydough.relational.relational_nodes import (
    Scan,
    Filter,
    Project,
    Limit,
    Join,
    RelationalRoot,
    JoinType,
)
from pydough.relational.relational_expressions import (
    ColumnReference,
    LiteralExpression,
)
from pydough.types import NumericType, BooleanType

# Create a scan node
scan_node = Scan("table_name", {"column_name": ColumnReference("column_name", NumericType())})

# Create a filter node
filter_condition = LiteralExpression(True, BooleanType())
filter_node = Filter(scan_node, filter_condition, scan_node.columns)

# Create a project node
project_node = Project(filter_node, {"column_name": ColumnReference("column_name", NumericType())})

# Create a limit node
limit_expression = LiteralExpression(10, NumericType())
limit_node = Limit(project_node, limit_expression, project_node.columns)

# Create a join node
join_condition = LiteralExpression(True, BooleanType())
join_node = Join([scan_node, filter_node], [join_condition], [JoinType.INNER], scan_node.columns)

# Create a relational root node
root_node = RelationalRoot(limit_node, [("column_name", ColumnReference("column_name", NumericType()))])

# Convert the root node to a string
root_node_str = root_node.to_string()
```