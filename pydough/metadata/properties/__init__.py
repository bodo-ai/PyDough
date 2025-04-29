"""
Submodule of the PyDough metadata module defining metadata for properties.
"""

__all__ = [
    "CartesianProductMetadata",
    "GeneralJoinMetadata",
    "PropertyMetadata",
    "ReversiblePropertyMetadata",
    "ScalarAttributeMetadata",
    "SimpleJoinMetadata",
    "SubcollectionRelationshipMetadata",
    "TableColumnMetadata",
]

from .cartesian_product_metadata import CartesianProductMetadata
from .general_join_metadata import GeneralJoinMetadata
from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata
from .scalar_attribute_metadata import ScalarAttributeMetadata
from .simple_join_metadata import SimpleJoinMetadata
from .subcollection_relationship_metadata import SubcollectionRelationshipMetadata
from .table_column_metadata import TableColumnMetadata
