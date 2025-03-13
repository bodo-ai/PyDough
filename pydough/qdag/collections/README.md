# PyDough QDAG Collections Module

This module deals with the qualified DAG (QDAG) structure used as an intermediary representation after unqualified nodes and before the relational tree. The QDAG nodes represent collections and expressions in a structured manner that can be further processed and transformed.

## Available APIs

The QDAG collections module contains the following hierarchy of collection classes:

- [`PyDoughCollectionQDAG`](collection_qdag.py) (abstract): Base class for all collection QDAG nodes.
    - [`GlobalContext`](global_context.py) (abstract): The global context of the graph.
    - [`ChildAccess`](child_access.py) (abstract): Accessing a child collection.
        - [`ChildReferenceCollection.py`](child_reference_collection.py) (concrete): Accessing a collection placed as a child of a child operator just so the entire collection can be referenced (e.g. calling `COUNT` on a subcollection).
        - [`ChildOperatorChildAccess`](child_operator_child_access.py) (concrete): Wrapper around another child access instance that is used when the child access is happening to make it a child context of a child operator instead of stepping into the child directly (e.g. `nations` in `Region(COUNT(nations))` instead of `Region.nations`).
            - [`PartitionChild`](partition_child.py) (concrete): Specifically a reference to the child data of a `PartitionBy` node.
        - [`CollectionAccess`](collection_access.py) (abstract): Accessing a child collection specifically via a collection/subcollection property.
            - [`TableCollection`](table_collection.py) (concrete): Accessing a table collection directly.
            - [`SubCollection`](sub_collection.py) (concrete): Accessing a subcolleciton of another collection.
                - [`CompoundSubCollection`](sub_collection.py) (concrete): Accessing a subcollection of another collection where the subcollection property is a compound relationship.
    - [`ChildOperator`](child_operator.py) (abstract): Base class for collection QDAG nodes that need to access child contexts in order to make a child reference.
        - [`Calculate`](calculate.py) (concrete): Operation that defines new singular expression terms in the current context and names them.
        - [`Where`](where.py) (concrete): Operation that filters the current context based on a predicate that is a singular expression.
        - [`OrderBy`](order_by.py) (concrete): Operation that sorts the current context based on 1+ singular collation expressions.
            - [`TopK`](top_k.py) (concrete): Operation that sorts the current context based on 1+ singular collation expressions and filters to only keep the first `k` records.
        - [`PartitionBy`] (concrete): Operation that partitions the its child data by its partition keys, creating a new parent collection where the expressions are the unique combinations of partition key values  and there is a single subcollection pointing back to the original data.
        - [`Singular`](singular.py) (concrete): Operation that annotates the preceding context so it is known to be singular with regards to a parent context.

The base QDAG collection node contains the following interface:

- `ancestor_context`: Property that returns the ancestor QDAG collection node, if one exists. For example, in `Nations.WHERE(region.name == "ASIA").suppliers`, the ancestor is `Nations.WHERE(region.name == "ASIA")`.
- `preceding_context`: Property that returns the predecessor QDAG collection node, if one exists. For example, in `Nations.WHERE(region.name == "ASIA").TOP_K(3, by=name.ASC())`, the predecessor is `Nations.WHERE(region.name == "ASIA")`.
- `calc_terms`: Property that returns the set of all names of terms in the collection that should be included in the final output when the collection is converted to SQL and/or executed.
- `all_terms`: Property that returns the set of all names of terms of the collection (collections or expressions).
- `is_singular`: Method that takes in a context and returns whether the current collection is singular with regards to that context. (Note: it is assumed that `.starting_predecessor` has been called on all the arguments already).
- `starting_predecessor`: Property that finds the furthest predecessor of the curren collection.
- `verify_singular_terms`: Method that takes in a sequence of expression QDAG nodes and verifies that all of them are singular with regards to the current context (e.g. can they be used as CALCULATE terms).
- `get_expression_position`: Method that takes in the string name of a calculate term and returns its ordinal position when placed in the output.
- `get_term`: Method that takes in the string name of any term of the current context and returns the QDAG node for it with regards to the current context. E.g. if calling on the name of a subcollection, returns the subcollection node.
- `get_expr`: Same as `get_term` but specifically for expressions-only.
- `get_collection`: Same as `get_term` but specifically for collections-only.
- `ordering`: Property that returns the list of terms that a collection is sorted by, if one exists (gh #164: remove this and replace with a different system).
- `standalone_string`: Property that returns a string representation of the node within without any context of its predecessors/ancestors.
- `to_string`: Method that returns a string representation of the node in a format similar to the original PyDough code.
- `to_tree_string`: Method that converts the QDAG node into a tree-like string representation. Has the following helper utilities:
    - `tree_item_string`: Property that converts the current collection into a 1-line string used in a single row of the tree string.
    - `to_tree_form`: Method that converts the current collection and its full ancestry to tree form objects.
    - `to_tree_form_isolated`: Method that converts the current collection into its tree form without hooking it up to its ancestors.

### Collection Tree Form

The `CollectionTreeForm` class is a helper utility class used to describe PyDough QDAG collections in a tree-like form so they can be converted into tree-like strings. It is used by the `to_tree_string` method of `PyDoughCollectionQDAG` to generate a tree-like string representation of the collection and its ancestors.

The objects are created by calling the `to_tree_form` API of a collection QDAG node, and are converted to a list of strings with the `to_string_rows` API.

#### Example

Below is an example of a PyDough snippet and the corresponding tree string representation:

```python
Nations.CALCULATE(
    nation_name=name,
).WHERE(
    region.name == "EUROPE"
).suppliers.CALCULATE(
    supplier_name=name,
    nation_name=nation_name,
)
```

```
──┬─ TPCH
  ├─── TableCollection[Nations]
  ├─── Calculate[nation_name=name]
  └─┬─ Where[$1.name == 'EUROPE']
    ├─┬─ AccessChild
    │ └─── SubCollection[region]
    ├─── SubCollection[suppliers]
    └─── Calculate[supplier_name=name, nation_name=nation_name]
```

And below is another such example:

```python
german_suppliers = supply_records.WHERE(supplier.nation == "GERMANY")
selected_parts = parts.WHERE(HAS(german_suppliers))
PARTITION(selected_parts, name="p", by=size).CALCULATE(
    size,
    n_parts_with_german_supplier=COUNT(p)
).TOP_K(
    10, 
    by=n_parts_with_german_supplier.DESC()
)
```

```
──┬─ TPCH
  ├─┬─ Partition[name='p', by=size]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Parts]
  │   └─┬─ Where[HAS($1)]
  │     └─┬─ AccessChild
  │       └─┬─ SubCollection[supply_records]
  │         └─┬─ Where[$1.name == "GERMANY"]
  │           └─┬─ AccessChild
  │             └─┬─ SubCollection[supplier]
  │               └─── SubCollection[nation]
  ├─┬─ Calculate[size=size, n_parts_with_german_supplier=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[p]
  └─── TopK[10, n_parts_with_german_supplier.DESC(na_pos='last')]
```
