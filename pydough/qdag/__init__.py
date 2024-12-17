"""
Module of PyDough dealing with the qualified DAG structure (aka QDAG) used as
an intermediary representation after unqualified nodes and before the
relational tree.
"""

__all__ = [
    "PyDoughQDAG",
    "PyDoughExpressionQDAG",
    "AstNodeBuilder",
    "ColumnProperty",
    "Literal",
    "ExpressionFunctionCall",
    "PyDoughASTException",
    "PyDoughCollectionQDAG",
    "TableCollection",
    "SubCollection",
    "Calc",
    "ChildOperatorChildAccess",
    "Where",
    "CollationExpression",
    "OrderBy",
    "PartitionBy",
    "ChildReferenceExpression",
    "ChildAccess",
    "TopK",
    "GlobalContext",
    "ChildReferenceCollection",
    "Reference",
    "ChildOperator",
    "CompoundSubCollection",
    "PartitionChild",
    "CollectionAccess",
    "BackReferenceExpression",
    "PartitionKey",
    "BackReferenceCollection",
]

from .abstract_pydough_qdag import PyDoughQDAG
from .collections import (
    BackReferenceCollection,
    Calc,
    ChildAccess,
    ChildOperator,
    ChildOperatorChildAccess,
    ChildReferenceCollection,
    CollectionAccess,
    CompoundSubCollection,
    GlobalContext,
    OrderBy,
    PartitionBy,
    PartitionChild,
    PyDoughCollectionQDAG,
    SubCollection,
    TableCollection,
    TopK,
    Where,
)
from .errors import PyDoughASTException
from .expressions import (
    BackReferenceExpression,
    ChildReferenceExpression,
    CollationExpression,
    ColumnProperty,
    ExpressionFunctionCall,
    Literal,
    PartitionKey,
    PyDoughExpressionQDAG,
    Reference,
)
from .node_builder import AstNodeBuilder
