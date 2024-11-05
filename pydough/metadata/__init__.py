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
]

from .collections import CollectionMetadata, SimpleTableMetadata
from .errors import PyDoughMetadataException
from .graphs import GraphMetadata
from .parse import parse_json_metadata_from_file
from .properties import (
    CartesianProductMetadata,
    CompoundRelationshipMetadata,
    PropertyMetadata,
    SimpleJoinMetadata,
    TableColumnMetadata,
)
