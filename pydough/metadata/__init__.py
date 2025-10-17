"""
Module of PyDough dealing with definitions and parsing of PyDough metadata.
"""

__all__ = [
    "CartesianProductMetadata",
    "CollectionMetadata",
    "GeneralJoinMetadata",
    "GraphMetadata",
    "MaskedTableColumnMetadata",
    "PropertyMetadata",
    "SimpleJoinMetadata",
    "SimpleTableMetadata",
    "SubcollectionRelationshipMetadata",
    "TableColumnMetadata",
    "parse_json_metadata_from_file",
]

from .collections import CollectionMetadata, SimpleTableMetadata
from .graphs import GraphMetadata
from .parse import parse_json_metadata_from_file
from .properties import (
    CartesianProductMetadata,
    GeneralJoinMetadata,
    MaskedTableColumnMetadata,
    PropertyMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
    TableColumnMetadata,
)
