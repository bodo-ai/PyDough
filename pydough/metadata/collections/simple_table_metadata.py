"""
Definition of PyDough metadata for a collection that trivially corresponds to a
table in a relational system.
"""

from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    NoExtraKeys,
    PyDoughMetadataException,
    is_string,
    unique_properties_predicate,
)
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.properties import (
    CartesianProductMetadata,
    CompoundRelationshipMetadata,
    GeneralJoinMetadata,
    InheritedPropertyMetadata,
    PropertyMetadata,
    SimpleJoinMetadata,
    TableColumnMetadata,
)

from .collection_metadata import CollectionMetadata


class SimpleTableMetadata(CollectionMetadata):
    """
    Concrete metadata implementation for a PyDough collection representing a
    relational table where the properties are columns to the table, or subsets
    of other such tables created from joins.
    """

    # Set of names of fields that can be included in the JSON
    # object describing a simple table collection.
    allowed_fields: set[str] = CollectionMetadata.allowed_fields | {
        "table_path",
        "unique_properties",
    }

    def __init__(
        self,
        name: str,
        graph,
        table_path: str,
        unique_properties: list[str | list[str]],
    ):
        super().__init__(name, graph)
        is_string.verify(table_path, f"Property 'table_path' of {self.error_name}")
        unique_properties_predicate.verify(
            unique_properties, f"property 'unique_properties' of {self.error_name}"
        )
        self._table_path: str = table_path
        self._unique_properties: list[str | list[str]] = unique_properties

    @property
    def table_path(self) -> str:
        """
        The path used to identify the table within whatever data storage
        mechanism is being used.
        """
        return self._table_path

    @property
    def unique_properties(self) -> list[str | list[str]]:
        """
        The list of all names of properties of the collection that are
        guaranteed to be unique within the collection. Entries that are a
        string represent a single column being completely unique, while entries
        that are a list of strings indicate that each combination of those
        properties is unique.
        """
        return self._unique_properties

    @staticmethod
    def create_error_name(name, graph_error_name):
        return f"simple table collection {name!r} in {graph_error_name}"

    @property
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.table_path)
        comp.append(self.unique_properties)
        return comp

    def verify_complete(self) -> None:
        # First do the more general checks
        super().verify_complete()

        # Extract all names properties used in the uniqueness of the table
        # collection, ensuring there are no invalid duplicates.
        malformed_unique_msg: str = f"{self.error_name} has malformed unique properties set: {self.unique_properties}"
        unique_property_combinations: set[tuple] = set()
        unique_property_names: set[str] = set()
        for unique_property in self.unique_properties:
            unique_property_set: set[str]
            if isinstance(unique_property, str):
                unique_property_set = {unique_property}
            else:
                unique_property_set = set(unique_property)
                if len(unique_property_set) < len(unique_property):
                    raise PyDoughMetadataException(malformed_unique_msg)
            unique_property_tuple: tuple = tuple(sorted(unique_property_set))
            if unique_property_tuple in unique_property_combinations:
                raise PyDoughMetadataException(malformed_unique_msg)
            unique_property_combinations.add(unique_property_tuple)
            unique_property_names.update(unique_property_set)

        # Ensure that each unique property exists as a scalar attribute of
        # the collection.
        for unique_property_name in unique_property_names:
            if unique_property_name not in self.properties:
                raise PyDoughMetadataException(
                    f"{self.error_name} does not have a property named {unique_property_name!r} to use as a unique property"
                )
            property = self.get_property(unique_property_name)
            assert isinstance(property, PropertyMetadata)
            if property.is_subcollection:
                raise PyDoughMetadataException(
                    f"{property.error_name} cannot be a unique property since it is a subcollection"
                )

    def verify_allows_property(
        self, property: AbstractMetadata, inherited: bool
    ) -> None:
        """
        Verifies that a property is safe to add to the collection.

        Args:
            `property`: the metadata for a PyDough property that is being
            added to the collection.
            `inherited`: True if verifying a property being inserted as an
            inherited property, False otherwise.

        Raises:
            `PyDoughMetadataException`: if `property` is not a valid property
            to insert into the collection.
        """
        # Invoke the more generic checks.
        super().verify_allows_property(property, inherited)

        # Ensure that the property is one of the supported types for this
        # type of collection.
        match property:
            case (
                TableColumnMetadata()
                | CartesianProductMetadata()
                | SimpleJoinMetadata()
                | CompoundRelationshipMetadata()
                | InheritedPropertyMetadata()
                | GeneralJoinMetadata()
            ):
                pass
            case _:
                raise PyDoughMetadataException(
                    f"Simple table collections does not allow inserting {property.error_name}"
                )

    @staticmethod
    def verify_json_metadata(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        Verifies that a JSON object contains well formed data to create a new simple
        table collection.

        Args:
            `graph`: the metadata for the graph that the collection would
            be added to.
            `collection_name`: the name of the collection that would be added
            to the graph.
            `collection_json`: the JSON object that is being verified to ensure
            it represents a valid collection.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """
        # Create the string used to identify the collection in error messages.
        error_name: str = SimpleTableMetadata.create_error_name(
            collection_name, graph.error_name
        )

        # Check that the JSON data contains the required properties
        # `table_path` and `unique_properties`, without any extra properties.
        HasPropertyWith("table_path", is_string).verify(collection_json, error_name)
        HasPropertyWith("unique_properties", unique_properties_predicate).verify(
            collection_json, error_name
        )
        NoExtraKeys(SimpleTableMetadata.allowed_fields).verify(
            collection_json, error_name
        )

    @staticmethod
    def parse_from_json(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        Parses a JSON object into the metadata for a simple table collection
        and inserts it into the graph.

        Args:
            `graph`: the metadata for the graph that the collection will be
            added to.
            `collection_name`: the name of the collection that will be added
            to the graph.
            `collection_json`: the JSON object that is being parsed to create
            the new collection.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """
        # Verify that the JSON is well structured.
        SimpleTableMetadata.verify_json_metadata(
            graph, collection_name, collection_json
        )

        # Extract the relevant properties from the JSON to build the new
        # collection, then add it to the graph.
        table_path: str = collection_json["table_path"]
        unique_properties: list[str | list[str]] = collection_json["unique_properties"]
        new_collection: SimpleTableMetadata = SimpleTableMetadata(
            collection_name, graph, table_path, unique_properties
        )
        graph.add_collection(new_collection)
