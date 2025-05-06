"""
Definition of the class for PyDough metadata for properties that access a
column of a table from a relational system.
"""

__all__ = ["TableColumnMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    NoExtraKeys,
    PyDoughMetadataException,
    extract_array,
    extract_object,
    extract_string,
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
        "data type",
        "column name",
        "sample values",
    }

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        data_type: PyDoughType,
        column_name: str,
        sample_values: list | None = None,
        description: str | None = None,
        synonyms: list[str] | None = None,
        extra_semantic_info: dict | None = None,
    ):
        super().__init__(
            name,
            collection,
            data_type,
            sample_values,
            description,
            synonyms,
            extra_semantic_info,
        )
        is_string.verify(column_name, "column name")
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
        error_name: str = TableColumnMetadata.create_error_name(
            property_name, collection.error_name
        )
        # Extract the `data_type` and `column_name` fields from the JSON object
        type_string: str = extract_string(property_json, "data type", error_name)
        try:
            data_type: PyDoughType = parse_type_from_string(type_string)
        except PyDoughTypeException as e:
            raise PyDoughMetadataException(*e.args)
        column_name: str = extract_string(property_json, "column name", error_name)

        NoExtraKeys(TableColumnMetadata.allowed_fields).verify(
            property_json, error_name
        )

        # Extract the optional fields from the JSON object.
        sample_values: list | None = None
        description: str | None = None
        synonyms: list[str] | None = None
        extra_semantic_info: dict | None = None
        if "sample values" in property_json:
            sample_values = extract_array(property_json, "sample values", error_name)
        if "description" in property_json:
            description = extract_string(property_json, "description", error_name)
        if "synonyms" in property_json:
            synonyms = extract_array(property_json, "synonyms", error_name)
        if "extra semantic info" in property_json:
            extra_semantic_info = extract_object(
                property_json, "extra semantic info", error_name
            )

        # Build the new property metadata object and add it to the collection.
        property: TableColumnMetadata = TableColumnMetadata(
            property_name,
            collection,
            data_type,
            column_name,
            sample_values,
            description,
            synonyms,
            extra_semantic_info,
        )
        collection.add_property(property)
