"""
TODO: add file-level docstring
"""

from .scalar_attribute_metadata import ScalarAttributeMetadata
from pydough.metadata.errors import (
    verify_has_type,
    verify_valid_name,
    verify_json_has_property_with_type,
)
from pydough.types import parse_type_from_string, PyDoughType
from pydough.metadata.collections import CollectionMetadata


class TableColumnMetadata(ScalarAttributeMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        data_type: PyDoughType,
        column_name: str,
    ):
        super().__init__(name, collection, data_type)
        verify_has_type(column_name, str, "column_name")
        self.column_name: str = column_name

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"table column property {name!r} of {collection_error_name}"

    @property
    def components(self) -> tuple:
        return super().components + (self.column_name,)

    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        verify_valid_name(property_name)
        # Create the string used to identify the property in error messages.
        error_name = TableColumnMetadata.create_error_name(
            property_name, collection.error_name
        )
        verify_json_has_property_with_type(
            property_json, "column_name", str, error_name
        )
        verify_json_has_property_with_type(property_json, "data_type", str, error_name)

    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        data_type = parse_type_from_string(property_json["data_type"])
        column_name = property_json["column_name"]
        property = TableColumnMetadata(
            property_name, collection, data_type, column_name
        )
        collection.add_property(property)
