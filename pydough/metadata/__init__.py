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
    "ScalarAttributeMetadata",
    "SimpleJoinMetadata",
    "SimpleTableMetadata",
    "SubcollectionRelationshipMetadata",
    "TableColumnMetadata",
    "parse_json_metadata_from_file",
    "parse_metadata_from_list",
]

from .collections import CollectionMetadata, SimpleTableMetadata
from .graphs import GraphMetadata
from .parse import parse_json_metadata_from_file, parse_metadata_from_list
from .properties import (
    CartesianProductMetadata,
    GeneralJoinMetadata,
    MaskedTableColumnMetadata,
    PropertyMetadata,
    ScalarAttributeMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
    TableColumnMetadata,
)
