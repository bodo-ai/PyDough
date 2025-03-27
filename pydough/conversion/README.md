# Conversion

This subdirectory of the PyDough directory deals with the conversion of PyDough expressions into various intermediate representations.

The conversion module provides functionality to convert qualified DAG (QDAG) nodes into hybrid nodes and then into relational nodes.

## Hybrid Tree

The `HybridTree` is an intermediate representation used to bridge the gap between QDAG nodes and relational nodes. The components of this structure, and the process to convert QDAG nodes into it, are defined in [hybrid_tree.py](hybrid_tree.py)


Hybrid tree instances are structured as a linear chain linked to one another as predecessors/successors, and each hybrid tree instance is composed of a pipeline of hybrid operation instances. Each hybrid operation builds on the previous one, and the first one in the pipeline builds on the result from the last element in the pipeline of the predecessor. Steps from one hybrid tree down to its successor correspond to moving into a child context, whereas operations in the same pipeline operate on the same context by augmenting it.

### Hybrid Expressions

Hybrid expressions are used to represent various types of expressions within the hybrid tree. Some notable classes include:

- `HybridLiteralExpr`: Represents a literal value.
- `HybridColumnExpr`: Represents a column reference.
- `HybridFunctionExpr`: Represents a regular or aggregate function call.
- `HybridWindowExpr`: represents a window function call.
- `HybridRefExpr`: Represents a reference to another expression.
- `HybridBackRefExpr`: Represents a reference to another hybrid expression that exists in one of the successors of the current hybrid tree.
- `HybridChildRefExpr`: Represents a reference to another hybrid expression that exists in one of the child connections of the current hybrid tree.
- `HybridCorrelExpr`: Represents a reference to another hybrid expression that exists in the parent hybrid tree containing the current hybrid tree as a child subtree. These references are correlated because they mean that one side of a join will depend on logic from the other side.

### Hybrid Connections

Hybrid connections represent the relationships between different parts of the hybrid tree. They are used to handle subcollection accesses and aggregations. Its fields are as follows:

- `parent`: The `HybridTree` that the connection exists within.
- `subtree`: The `HybridTree` corresponding to the child itself, starting from the bottom.
- `connection_type`: An enum indicating which connection type is being used.
- `required_steps`: An index indicating which step in the pipeline must be completed before the child can be defined.
- `aggs`: A mapping of aggregation calls made onto expressions relative to the context of `subtree`.

The `ConnectionType` eEnum describes how a hybrid tree is connected to a child tree. It has the following values:

- `SINGULAR`: The child should be 1:1 with regards to the parent.
- `AGGREGATION`: The child is being accessed for the purposes of aggregating its columns.
- `NDISTINCT`: The child is being accessed for the purposes of counting how many distinct elements it has.
- `SEMI`: The child is being used as a semi-join.
- `SINGULAR_ONLY_MATCH`: A variant of `SINGULAR` that can use an INNER join instead of a LEFT join.
- `AGGREGATION_ONLY_MATCH`: A variant of `AGGREGATION` that can use an INNER join instead of a LEFT join.
- `NDISTINCT_ONLY_MATCH`: A variant of `NDISTINCT` that can use an INNER join instead of a LEFT join.
- `ANTI`: The child is being used as an anti-join.
- `NO_MATCH_SINGULAR`: A variant of `SINGULAR` that replaces all of the child references with NULL.
- `NO_MATCH_AGGREGATION`: A variant of `AGGREGATION` that replaces all of the aggregation outputs with NULL.
- `NO_MATCH_NDISTINCT`: A variant of `NDISTINCT` that replaces the NDISTINCT output with 0.

### Hybrid Operations

Hybrid operations represent the various operations that can be performed within the hybrid tree. Some notable classes include:

- `HybridOperation`: Base class for all hybrid operations.
- `HybridRoot`: Represents the root context.
- `HybridCollectionAccess`: Represents accessing a collection.
- `HybridFilter`: Represents a filter operation.
- `HybridCalc`: Represents a calculation operation.
- `HybridLimit`: Represents a limit operation.
- `HybridPartition`: Represents a partition operation.
- `HybridPartitionChild`: Represents accessing the data of a partition as a child.
- `HybridNoop`: Represents a do-nothing operation that propagates all of the data from the previous operation without any change.
- `HybridChildPullup`: Represents an operation that accesses all of the data from a child hybrid tree as if one of its levels were the current level and its ancestors were the current levels ancestors, while the bottom level remain as child references.

## Hybrid De-Correlation

The file [hybrid_decorrelater.py](hybrid_decorrelater.py) contains the logic used to hunt for `HybridCorrelExpr` outside of a semi/anti join and de-correlates them by snapshotting the hybrid tree and attaching the snapshot copy to the top of the child subtree, thus turning the correlated reference into a back reference. Also, for only-match patterns, replaces the original logic that was snapshotted with a single `HybridChildPullup` to avoid re-computing the same logic twice.

## Relational Conversion

The methodology for converting hybrid nodes into relational nodes is implemented in [relational_converter.py](relational_converter.py). The main steps involved in this process are:

1. **Preprocessing**: The QDAG node is preprocessed to ensure that all necessary terms are included in the final output.
2. **Hybrid Conversion**: The QDAG node is converted into a hybrid tree structure.
3. **Relational Translation**: The hybrid nodes are recursively converted into relational nodes by starting at the last pipeline operator of the bottom of the hybrid tree, working backwards, and building onto the previous result. The accumulated result is both the relational tree of the answer so far and a mapping of hybrid expressions to the corresponding column in the answer that corresponds to their value. Operations such as filter or limit just wrap the previous result in another relational node, but several other translations have more steps:
   - When stepping down from one level of the HybridTree to the next level, e.g. by accessing a subcollection, the result of the previous level is inner joined to reach the new level, and all expressions from the previous level are back-shifted up by 1.
   - All child references are resolved by calculating the relational translation of the child and joining it onto the current result so that its expressions are accessible as child expressions. The type of join depends on the type of `HybridConnection`, but the result is that every row in the current relation matches to at most 1 row in the child relational tree, either because it is already guaranteed to be singular or because it has been aggregated in a manner that ensures so.
4. **Finalization**: The final relational structure is created, and extra optimizations are run such as filter pushdown and column pruning.

## Hybrid De-Correlation

The file [filter_pushdown.py](filter_pushdown.py) contains the logic used to push filters further down into the relational tree.
