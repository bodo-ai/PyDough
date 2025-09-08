"""
Definition of the class for PyDough metadata for properties that access a
column of a table from a relational system that has been masked by a certain
protocol, and includes an associated unmasking protocol to reverse the operation.
"""

__all__ = ["MaskedTableColumnMetadata"]


from pydough.errors import (
    PyDoughMetadataException,
    PyDoughTypeException,
)
from pydough.errors.error_utils import (
    NoExtraKeys,
    extract_bool,
    extract_string,
)
from pydough.metadata.collections import CollectionMetadata
from pydough.types import PyDoughType, parse_type_from_string

from .table_column_metadata import TableColumnMetadata


class MaskedTableColumnMetadata(TableColumnMetadata):
    """
    Concrete metadata implementation for a PyDough property representing a
    column of data in a relational table that where the data is masked by a specific
    protocol, and can be unmasked using a defined unmasking protocol.
    """

    # Set of names of fields that can be included in the JSON object
    # describing a table column property.
    allowed_fields: set[str] = TableColumnMetadata.allowed_fields | {
        "protected data type",
        "protect protocol",
        "unprotect protocol",
        "server masked",
    }

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        data_type: PyDoughType,
        protected_data_type: PyDoughType,
        column_name: str,
        unprotect_protocol: str,
        protect_protocol: str,
        server_masked: bool,
        sample_values: list | None = None,
    ):
        super().__init__(
            name,
            collection,
            protected_data_type,
            column_name,
            sample_values,
        )
        self._unprotected_data_type: PyDoughType = data_type
        self._unprotect_protocol: str = unprotect_protocol
        self._protect_protocol: str = protect_protocol
        self._server_masked: bool = server_masked

    @property
    def unprotected_data_type(self) -> PyDoughType:
        """
        Returns the data type of the column when it is unprotected.
        """
        return self._unprotected_data_type

    @property
    def unprotect_protocol(self) -> str:
        """
        Returns the format string used to un-mask the data in this column.
        Call `self.unprotect_protocol.format(value)` to generate SQL for unmasking, where `value` is the SQL text string corresponding to a value that is masked.
        """
        return self._unprotect_protocol

    @property
    def protect_protocol(self) -> str:
        """
        Returns the format string used to mask the data in this column.
        Should be reversible by `self.unprotect_protocol`.
        """
        return self._protect_protocol

    @property
    def server_masked(self) -> bool:
        """
        Returns whether the data in this table column property is masked in
        a manner that corresponds to a server that can be sent predicate queries
        in order to infer smarter predicates that do not require un-masking the
        data.
        """
        return self._server_masked

    @staticmethod
    def create_error_name(name: str, collection_error_name: str) -> str:
        return f"masked table column property {name!r} of {collection_error_name}"

    @property
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.unprotected_data_type)
        comp.append(self.unprotect_protocol)
        comp.append(self.protect_protocol)
        comp.append(self.server_masked)
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
        error_name: str = MaskedTableColumnMetadata.create_error_name(
            property_name, collection.error_name
        )
        # Extract the `data_type` and `column_name` fields from the JSON object
        type_string: str = extract_string(property_json, "data type", error_name)
        protected_data_type: PyDoughType
        try:
            data_type: PyDoughType = parse_type_from_string(type_string)
            if "protected data type" in property_json:
                type_string = extract_string(
                    property_json, "protected data type", error_name
                )
                protected_data_type = parse_type_from_string(type_string)
            else:
                protected_data_type = data_type
        except PyDoughTypeException as e:
            raise PyDoughMetadataException(*e.args)
        column_name: str = extract_string(property_json, "column name", error_name)

        # Extract the `unprotect protocol`, `protect protocol`, and
        # `server masked` fields from the JSON object.
        unprotect_protocol: str = extract_string(
            property_json, "unprotect protocol", error_name
        )
        protect_protocol: str = extract_string(
            property_json, "protect protocol", error_name
        )
        server_masked: bool = False
        if "server masked" in property_json:
            server_masked = extract_bool(property_json, "server masked", error_name)

        NoExtraKeys(MaskedTableColumnMetadata.allowed_fields).verify(
            property_json, error_name
        )

        # Build the new property metadata object and add it to the collection.
        property: MaskedTableColumnMetadata = MaskedTableColumnMetadata(
            property_name,
            collection,
            data_type,
            protected_data_type,
            column_name,
            unprotect_protocol,
            protect_protocol,
            server_masked,
        )
        # Parse the optional common semantic properties like the description.
        property.parse_optional_properties(property_json)
        collection.add_property(property)
