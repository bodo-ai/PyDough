"""
Definition of the class for PyDough metadata for properties that access a
column of a table from a relational system.
"""

__all__ = ["TableColumnMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    NoExtraKeys,
    PyDoughMetadataException,
    is_string,
)
from pydough.types import PyDoughType, parse_type_from_string
from pydough.types.errors import PyDoughTypeException

from .property_metadata import PropertyMetadata
from .scalar_attribute_metadata import ScalarAttributeMetadata


class TableColumnMetadata(ScalarAttributeMetadata):
    """
    Concrete metadata implementation for a PyDough property representing a
    column of data from a relational table.
    """

    # Set of names of fields that can be included in the JSON object
    # describing a table column property.
    allowed_fields: set[str] = PropertyMetadata.allowed_fields | {
        "data_type",
        "column_name",
    }

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        data_type: PyDoughType,
        column_name: str,
    ):
        super().__init__(name, collection, data_type)
        is_string.verify(column_name, "column_name")
        self._column_name: str = column_name

    @property
    def column_name(self) -> str:
        return self._column_name

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"table column property {name!r} of {collection_error_name}"

    @property
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.column_name)
        return comp

    @staticmethod
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
            property would be inserted into.
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
        HasPropertyWith("column_name", is_string).verify(property_json, error_name)
        HasPropertyWith("data_type", is_string).verify(property_json, error_name)
        NoExtraKeys(TableColumnMetadata.allowed_fields).verify(
            property_json, error_name
        )

    @staticmethod
    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure dispatched from PropertyMetadata.parse_from_json to handle
        the parsing for table column properties.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted into.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new table column property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Extract the `data_type` and `column_name` fields from the JSON object
        type_string: str = property_json["data_type"]
        try:
            data_type: PyDoughType = parse_type_from_string(type_string)
        except PyDoughTypeException as e:
            raise PyDoughMetadataException(*e.args)
        column_name: str = property_json["column_name"]

        # Build the new property metadata object and add it to the collection.
        property: TableColumnMetadata = TableColumnMetadata(
            property_name, collection, data_type, column_name
        )
        collection.add_property(property)
