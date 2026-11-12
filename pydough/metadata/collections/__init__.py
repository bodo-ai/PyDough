"""
Submodule of the PyDough metadata module defining metadata for collections.
"""

__all__ = ["CollectionMetadata", "SQLQueryMetadata", "SimpleTableMetadata"]

from .collection_metadata import CollectionMetadata
from .simple_table_metadata import SimpleTableMetadata
from .sql_query_metadata import SQLQueryMetadata
