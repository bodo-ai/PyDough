# PyDough Internal Representations Guide

This document describes the various IRs used by PyDough to convert raw PyDough code into SQL text, as well as the conversion processes between them.

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Overview](#overview)
- [Unqualified Nodes](#unqualified-nodes)
  - [Unqualified Transform](#unqualified-transform)
- [QDAG Nodes](#qdag-nodes)
  - [Qualification](#qualification)
- [Hybrid Tree](#hybrid-tree)
  - [Hybrid Conversion](#hybrid-conversion)
  - [Hybrid Decorrelation](#hybrid-decorrelation)
- [Relational Tree](#relational-tree)
  - [Relational Conversion](#relational-conversion)
  - [Relational Optimization](#relational-conversion)
- [SQLGlot AST](#sqlglot-ast)
  - [SQLGlot Conversion](#sqlglot-conversion)
  - [SQLGlot Optimization](#sqlglot-optimization)

<!-- TOC end -->

<!-- TOC --><a name="overview"></a>
## Overview

The overarching pipeline for converting PyDough Python text into SQL text is as follows:
1. The Python text is intercepted and re-written in a [transformation](#unqualified-transform) that replaces undefined variable names with certain objects, ensuring that when the Python code is executed the result is an [unqualified node](#unqualified-nodes). These unqualified nodes are very minimal in terms of information stored,
2. When `to_sql` or `to_df` is called on this unqualified nodes, it is first sent through a process called [qualification](#qualification) which converts unqualified nodes into qualified DAG nodes, or [QDAG nodes](#qdag) for short. These QDAG nodes utilize the metadata in order to correctly associate every aspect of the PyDough logic with the data being analyzed/transformed, and is also where a great deal of the verification of the PyDough code's validity happens.
3. Next, the QDAG nodes are run through a process called [hybrid conversion](#hybrid-conversion) which restructures the logic into the datastructure known as the [hybrid tree](#hybrid-tree) in order to better organize the types of ways different subtrees of the data are linked together.
4. The hybrid tree is further transformed by the [decorrelation procedure](#hybrid-decorrelation) to remove correlated references created by the hybrid conversion process.
5. The transformed hybrid tree is converted into a [relational tree](#relational-tree) highly reminiscent of the datastructure used to represent relational algebra in frameworks such as Apache Calcite. The conversion to this datastructure is called [relational conversion](#relational-conversion).
6. Several [optimizations](#relational-optimization) are performed on the relational tree to combine/delete/split/transpose relational nodes, resulting in plans that are better for performance and/or visual quality when converted to SQL.
7. The Relational tree is [converted](#sqlglot-conversion) into the [internal AST](#sqlglot-ast) used by the open source Python library SQLGlot. This library is used for transpiling between different SQL dialects, so it is trivial to convert the SQLglot AST into SQL text of many different dialects.
8. The SQLGlot AST is [simplified & optimized](#sqlglot-optimization) less so to improve the performance of the SQL when executed, and moreso to improve the visual quality when it is converted into text.
9. A simple method call on the final SQLGlot AST converts it into SQL text of the desired dialect, which can then be executed via a database connector API.

To recap, the overall pipeline is as follows (if viewing with VSCode preview, you must install the mermaid markdown extension):
```mermaid
flowchart TD
    A[Python
    Text] -->|"Unqualified
    Transform"| B(Unqualified
    Node)
    B -->|Qualification| C[QDAG
    Node]
    B'[Metadata] -->C
    C -->|"Hybrid
    Conversion"| D[Hybrid Tree]
    D <--> D'{{Hybrid
    Decorrelation}}
    D --> E[Relational
    Tree]
    E <--> E'{{Relational
    Optimiation}}
    F <--> F'{{SQLGlot
    Optimization}}
    E -->|SQLGlot
    Conversion| F[SQLGlot
    AST]
    F --> G[SQL
    Text]
```


<!-- TOC --><a name="unqualified-nodes"></a>
## Unqualified Nodes

TODO

Below is an example of the structure of unqualified nodes for the following PyDough expression:
```py
nation_info = nations.calculate(
  region_name=region.name,
  nation_name=name,
  n_customers_in_debt=COUNT(customers.WHERE(acctbal < 0)))
.ORDER_BY(nation_name.ASC())
```

```mermaid
flowchart RL
    A[OrderBy] --> B[Calculate]
    B --> C[Access:
    'nations']
    C --> D[ROOT]
    B -.-> B1A[Access:
    'name']
    B1A --> B1B[Access:
    'region']
    B1B --> B1C[ROOT]
    B -.-> B2A[Access:
    'name']
    B2A --> B2B[ROOT]
    B -.-> B3A[Call:
    'COUNT']
    B3A --> B3B[Where]
    B3B --> B3C[Access:
    'customers']
    B3C --> B3D[ROOT]
    B3B -.-> P1[Call:
    '<']
    P1 --> P2[Access:
    'acctbal']
    P2 --> P3[ROOT]
    P1 --> P4[Literal:
    0]
    A -.-> A1[Collation:
    Ascending]
    A1 --> A2[Access:
    'nation_name'
    ]
    A2 --> A3[ROOT]
```

In the example above, `nation_info` refers to the `OrderBy` node on the right side of the diagram.

<!-- TOC --><a name="unqualified-transform"></a>
### Unqualified Transform

TODO

<!-- TOC --><a name="qdag-nodes"></a>
## QDAG Nodes

TODO

<!-- TOC --><a name="qualification"></a>
### Qualification

TODO

<!-- TOC --><a name="hybrid-tree"></a>
## Hybrid Tree

TODO

<!-- TOC --><a name="hybrid-conversion"></a>
### Hybrid Conversion

TODO

<!-- TOC --><a name="hybrid-decorrelation"></a>
### Hybrid Decorrelation

TODO

<!-- TOC --><a name="relational-tree"></a>
## Relational Tree

TODO

<!-- TOC --><a name="relational-conversion"></a>
### Relational Conversion

TODO

<!-- TOC --><a name="relational-optimization"></a>
### Relational Optimization

TODO

<!-- TOC --><a name="sqlglot-ast"></a>
## SQLGlot AST

TODO

<!-- TOC --><a name="sqlglot-conversion"></a>
### SQLGlot Conversion

TODO

<!-- TOC --><a name="sqlglot-optimization"></a>
### SQLGlot Optimization

TODO
