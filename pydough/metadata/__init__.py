"""
Module of PyDough dealing with definitions and parsing of PyDough metadata.

Copyright (C) 2024 Bodo Inc. All rights reserved.
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
    SubcollectionRelationshipMetadata,
    TableColumnMetadata,
)
