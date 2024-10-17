"""
TODO: add file-level docstring
"""

from typing import List
from . import PropertyMetadata
from .scalar_attribute_metadata import ScalarAttributeMetadata
from pydough.metadata.errors import (
    verify_has_type,
    verify_json_has_property_with_type,
    verify_no_extra_keys_in_json,
)
from pydough.types import parse_type_from_string, PyDoughType
from pydough.metadata.collections import CollectionMetadata


class TableColumnMetadata(ScalarAttributeMetadata):
    """
    Concrete metadata implementation for a PyDough property representing a
    column of data from a relational table.
    """

    # List of names of of fields that can be included in the JSON object
    # describing a table column property.
    allowed_fields: List[str] = PropertyMetadata.allowed_fields + [
        "data_type",
        "column_name",
    ]

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        data_type: PyDoughType,
        column_name: str,
    ):
        super().__init__(name, collection, data_type)
        verify_has_type(column_name, str, "column_name")
        self._column_name: str = column_name

    @property
    def column_name(self) -> str:
        return self.column_name

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"table column property {name!r} of {collection_error_name}"

    @property
    def components(self) -> tuple:
        return super().components + (self.column_name,)

    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Verifies that the JSON describing the metadata for a property within
        a collection is well-formed to create a new TableColumnMetadata instance.
        Should be dispatched from PropertyMetadata.verify_json_metadata which
        implements more generic checks.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted nto.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Create the string used to identify the property in error messages.
        error_name = TableColumnMetadata.create_error_name(
            property_name, collection.error_name
        )
        # Verify that the property has the required `column_name` and
        # `data_type` fields, without anything extra.
        verify_json_has_property_with_type(
            property_json, "column_name", str, error_name
        )
        verify_json_has_property_with_type(property_json, "data_type", str, error_name)
        verify_no_extra_keys_in_json(
            property_json, TableColumnMetadata.allowed_fields, error_name
        )

    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure dispatched from PropertyMetadata.parse_from_json to handle
        the parsing for table column properties.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted nto.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new table column property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Extract the `data_type` and `column_name` fields from the JSON object
        data_type: PyDoughType = parse_type_from_string(property_json["data_type"])
        column_name: str = property_json["column_name"]

        # Build the new property metadata object and add it to the collection.
        property: TableColumnMetadata = TableColumnMetadata(
            property_name, collection, data_type, column_name
        )
        collection.add_property(property)
