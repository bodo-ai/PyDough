"""
TODO: add module-level docstring
"""

__all__ = [
    "parse_json_metadata_from_file",
    "GraphMetadata",
    "CollectionMetadata",
    "PropertyMetadata",
    "TableColumnMetadata",
    "PyDoughMetadataException",
    "SimpleJoinMetadata",
    "CartesianProductMetadata",
    "CompoundRelationshipMetadata",
    "SimpleTableMetadata",
    "SubcollectionRelationshipMetadata",
    "explain_meta",
]

from .collections import CollectionMetadata, SimpleTableMetadata
from .errors import PyDoughMetadataException
from .exploration import explain_meta
from .graphs import GraphMetadata
from .parse import parse_json_metadata_from_file
from .properties import (
    CartesianProductMetadata,
    CompoundRelationshipMetadata,
    PropertyMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
    TableColumnMetadata,
)
