"""
Definition of the class for PyDough metadata for properties that act as a macro
for a PyDough expression.
"""

__all__ = ["PyDoughExpressionColumnMetadata"]


from typing import Any

from pydough.errors import PyDoughMetadataException, PyDoughTypeException
from pydough.errors.error_utils import (
    NoExtraKeys,
    extract_string,
    is_string,
    is_valid_sql_name,
)
from pydough.metadata.collections import CollectionMetadata
from pydough.types import PyDoughType, parse_type_from_string

from .property_metadata import PropertyMetadata
from .scalar_attribute_metadata import ScalarAttributeMetadata


class PyDoughExpressionColumnMetadata(ScalarAttributeMetadata):
    """
    Concrete metadata implementation for a PyDough property representing an
    expression that is defined in terms of a PyDough expression.
    """

    # Set of names of fields that can be included in the JSON object
    # describing a table column property.
    allowed_fields: set[str] = PropertyMetadata.allowed_fields | {
        "data type",
        "expression",
        "sample values",
    }

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        data_type: PyDoughType,
        expression: str,
        sample_values: list | None,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
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
        is_string.verify(expression, "expression")
        self._expression: str = expression
        # This field is not defined until later, during qualification, when
        # the expression is parsed and the result is placed here.
        self._pydough_expression: Any = None

    @property
    def expression(self) -> str:
        return self._expression

    @property
    def pydough_expression(self) -> Any:
        return self._pydough_expression

    @staticmethod
    def create_error_name(name: str, collection_error_name: str) -> str:
        return f"pydough expression column property {name!r} of {collection_error_name}"

    @property
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.expression)
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
        error_name: str = PyDoughExpressionColumnMetadata.create_error_name(
            property_name, collection.error_name
        )
        # Extract the `data_type` and `column_name` fields from the JSON object
        type_string: str = extract_string(property_json, "data type", error_name)
        try:
            data_type: PyDoughType = parse_type_from_string(type_string)
        except PyDoughTypeException as e:
            raise PyDoughMetadataException(*e.args)
        expression: str = extract_string(property_json, "expression", error_name)
        is_valid_sql_name.verify(expression, error_name)

        NoExtraKeys(PyDoughExpressionColumnMetadata.allowed_fields).verify(
            property_json, error_name
        )

        # Build the new property metadata object and add it to the collection.
        property: PyDoughExpressionColumnMetadata = PyDoughExpressionColumnMetadata(
            property_name,
            collection,
            data_type,
            expression,
            None,
            None,
            None,
            None,
        )
        # Parse the optional common semantic properties like the description.
        property.parse_optional_properties(property_json)
        collection.add_property(property)
