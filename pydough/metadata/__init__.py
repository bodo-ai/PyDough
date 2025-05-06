"""
Module of PyDough dealing with definitions and parsing of PyDough metadata.
"""

__all__ = [
    "CartesianProductMetadata",
    "CollectionMetadata",
    "GeneralJoinMetadata",
    "GraphMetadata",
    "PropertyMetadata",
    "PyDoughMetadataException",
    "SimpleJoinMetadata",
    "SimpleTableMetadata",
    "SubcollectionRelationshipMetadata",
    "TableColumnMetadata",
    "parse_json_metadata_from_file",
]

from .collections import CollectionMetadata, SimpleTableMetadata
from .errors import PyDoughMetadataException
from .graphs import GraphMetadata
from .parse import parse_json_metadata_from_file
from .properties import (
    CartesianProductMetadata,
    GeneralJoinMetadata,
    PropertyMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
    TableColumnMetadata,
)
