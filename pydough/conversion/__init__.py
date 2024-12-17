"""
Module of PyDough dealing with converting the qualified PyDough QDAG nodes into
Relational nodes.
"""

__all__ = ["convert_ast_to_relational"]

from .relational_converter import convert_ast_to_relational
